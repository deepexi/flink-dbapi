import unittest
from unittest.mock import MagicMock, patch
from flink_api.album import find_album_by_id


class TestAlbum(unittest.TestCase):

    @patch('flink_dbapi.album.requests')
    def test_find_album_by_id_success(self, mock_requests):
        # mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'userId': 1,
            'id': 1,
            'title': 'hello',
        }
        mock_requests.get.return_value = mock_response
        self.assertEqual(find_album_by_id(1), 'hello')
