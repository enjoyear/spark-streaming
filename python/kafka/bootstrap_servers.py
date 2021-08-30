class KafkaClientBase:
    def __init__(self, bootstrap_servers: str, cert_path: str, key_password: str):
        self._bootstrap_servers = bootstrap_servers
        credential_path = cert_path
        self._ssl_config = {
            'security.protocol': 'SSL',
            'ssl.ca.location': '%s/ca-certificates.crt' % credential_path,
            'ssl.certificate.location': '%s/signed_cert.pem' % credential_path,
            'ssl.key.location': '%s/csr_key.pem' % credential_path,
            'ssl.key.password': key_password,
        }
