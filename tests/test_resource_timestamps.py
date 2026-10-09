"""Offline checks of the actual SQL-embedded handlers and their wiring."""
import ast
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
DDLS = ROOT / 'scripts' / 'function_ddls'


def load_handler(name):
    ddl = (DDLS / f'{name}.sql').read_text().format('test_eai', 'portal.example')
    source = ddl.split('$$')[1]
    http = Mock()
    http.post.return_value.status_code = 200
    http.post.return_value.json.return_value = {'success': True, 'result': {'id': 'resource-id'}}
    modules = {
        '_snowflake': SimpleNamespace(get_generic_secret_string=Mock(return_value='test-token')),
        'requests': SimpleNamespace(Session=Mock(return_value=http)),
    }
    namespace = {}
    with patch.dict(sys.modules, modules):
        exec(compile(source, str(DDLS / f'{name}.sql'), 'exec'), namespace)
    return namespace, http


class ResourceTimestampTests(unittest.TestCase):
    def test_publish_sends_utc_seconds_without_suffix(self):
        namespace, http = load_handler('resource_update')
        clock = Mock()
        clock.now.return_value = datetime(2026, 10, 9, 16, 30, 12, 123456, tzinfo=timezone.utc)
        namespace['datetime'] = clock
        result = namespace['resource_update']('resource-id', 'csv', 'https://files.example/data.csv')
        clock.now.assert_called_once_with(timezone.utc)
        self.assertEqual(json.loads(result), {'id': 'resource-id'})
        http.post.assert_called_once_with(
            'https://portal.example/api/action/resource_update',
            headers={'Authorization': 'test-token', 'X-CKAN-API-Key': 'test-token'},
            json={'id': 'resource-id', 'format': 'csv', 'url': 'https://files.example/data.csv',
                  'clear_upload': 'true', 'last_modified': '2026-10-09T16:30:12'},
            timeout=60,
        )

    def test_renewal_patches_only_url_and_preserves_old_or_null_date(self):
        for previous in (None, '2026-10-01T12:00:00'):
            with self.subTest(previous=previous):
                namespace, http = load_handler('resource_renew_url')
                resource = {'id': 'resource-id', 'last_modified': previous, 'description': 'Keep me'}

                def apply_patch(url, headers, json, timeout):
                    self.assertEqual(url, 'https://portal.example/api/action/resource_patch')
                    self.assertEqual(set(json), {'id', 'url'})
                    resource.update(json)
                    return SimpleNamespace(status_code=200, json=lambda: {'success': True, 'result': resource})

                http.post.side_effect = apply_patch
                result = json.loads(namespace['resource_renew_url']('resource-id', 'https://files.example/new-link'))
                self.assertEqual(result['last_modified'], previous)
                self.assertEqual(result['description'], 'Keep me')
                self.assertEqual(result['url'], 'https://files.example/new-link')
                http.post.assert_called_once()

    def test_api_errors_are_not_reported_as_success(self):
        for name, args in (
            ('resource_update', ('resource-id', 'csv', 'https://files.example/data.csv')),
            ('resource_renew_url', ('resource-id', 'https://files.example/data.csv')),
        ):
            for status in (200, 403):
                with self.subTest(name=name, status=status):
                    namespace, http = load_handler(name)
                    http.post.return_value.status_code = status
                    http.post.return_value.json.return_value = {'success': False, 'error': 'Rejected'}
                    self.assertEqual(json.loads(namespace[name](*args)), {'error': 'Rejected', 'status': status})

    def test_network_and_invalid_json_errors_are_returned(self):
        for name, args in (
            ('resource_update', ('resource-id', 'csv', 'https://files.example/data.csv')),
            ('resource_renew_url', ('resource-id', 'https://files.example/data.csv')),
        ):
            with self.subTest(name=name):
                namespace, http = load_handler(name)
                http.post.side_effect = TimeoutError('Request timed out')
                with self.assertLogs('python_logger', level='ERROR'):
                    self.assertEqual(json.loads(namespace[name](*args)), {'error': 'Request timed out'})
                http.post.side_effect = None
                http.post.return_value.json.side_effect = ValueError('Invalid JSON')
                with self.assertLogs('python_logger', level='ERROR'):
                    self.assertEqual(json.loads(namespace[name](*args)), {'error': 'Invalid JSON'})

    def test_renewal_does_not_export_or_feed_publish_stream(self):
        task = (DDLS / 'renew_urls_task.sql').read_text().format('unused')
        body = task.split('$$')[1].upper()
        self.assertIn('CONFIG.RESOURCE_RENEW_URL(', body)
        self.assertIn('604800', body)
        self.assertIn('WHERE PRESIGNED_URL IS NOT NULL', body)
        for prohibited in ('SP_UPDATE_RESOURCES', 'UNLOAD_TO_INTERNAL_STAGE', 'COPY INTO',
                           'UPDATE CORE.RESOURCES', 'CORE.RESOURCES_STREAM', 'LAST_MODIFIED'):
            self.assertNotIn(prohibited, body)

    def test_data_publish_calls_api_only_after_export(self):
        setup = (ROOT / 'scripts' / 'setup.sql').read_text()
        for procedure in ('CONFIG.SP_UPDATE_RESOURCES(tname string)', 'CONFIG.SP_UPDATE_RESOURCES_ALL()'):
            with self.subTest(procedure=procedure):
                body = setup.split('CREATE OR REPLACE PROCEDURE ' + procedure, 1)[1]
                body = body.split('CREATE OR REPLACE PROCEDURE ', 1)[0]
                self.assertLess(body.index('CALL config.unload_to_internal_stage('),
                                body.index('config.resource_update('))
                self.assertNotIn('config.resource_renew_url(', body)

    def test_new_installs_skip_renewal_until_udf_exists(self):
        setup = (ROOT / 'scripts' / 'setup.sql').read_text()
        body = setup.split('CREATE OR REPLACE PROCEDURE CONFIG.ensure_url_renewal_task()', 1)[1]
        body = body.split('GRANT USAGE', 1)[0]
        self.assertIn("SHOW USER FUNCTIONS LIKE 'RESOURCE_RENEW_URL'", body)
        self.assertLess(body.index('IF (has_renewal_udf = 0)'),
                        body.index("CALL CONFIG.create_vwh_objects('')"))

    def test_finalize_builds_all_udfs_before_renewal_task(self):
        setup = (ROOT / 'scripts' / 'setup.sql').read_text()
        source = setup.split('HANDLER = \'create_functions\'', 1)[1].split('$$')[1]
        namespace = {}
        exec(compile(source, 'FINALIZE', 'exec'), namespace)
        session = Mock()
        session.file.get_stream.side_effect = lambda filename: io.BytesIO(
            (ROOT / filename.lstrip('/')).read_bytes())
        self.assertEqual(namespace['create_functions'](session, 'test_eai', 'portal.example'),
                         'Finalization complete')
        statements = [call.args[0] for call in session.sql.call_args_list]
        udf_statements = [statement for statement in statements if statement.startswith('begin CREATE')]
        self.assertEqual(len(udf_statements), 4)
        for statement in udf_statements:
            ast.parse(statement.split('$$')[1])
        self.assertIn('config.resource_renew_url', udf_statements[-1])
        self.assertEqual(statements[-1], 'CALL config.ensure_url_renewal_task()')
        self.assertLess(setup.index('CALL CONFIG.rebuild_ckan_functions();'),
                        setup.index('CALL CONFIG.redeploy_tasks();'))


if __name__ == '__main__':
    unittest.main()
