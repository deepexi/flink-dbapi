from dataclasses import dataclass
import requests

_SCHEMA_HTTP = "http"


@dataclass
class SqlGatewaySession:
    def __init__(self, host_port: str, session_handle: str):
        self.host_port = host_port
        self.session_handle = session_handle

    def session_endpoint(self) -> str:
        return f"{_SCHEMA_HTTP}://{self.host_port}/v1/sessions/{self.session_handle}"

    def schema_host_port(self) -> str:
        return f"{_SCHEMA_HTTP}://{self.host_port}"

    @staticmethod
    def _test_session(host_port: str = "127.0.0.1:8083"):
        r = requests.post(f"{_SCHEMA_HTTP}://{host_port}/v1/sessions", '{"sessionName" : "hehe"}')
        session_handle = r.json()["sessionHandle"]
        return SqlGatewaySession(host_port, session_handle)
