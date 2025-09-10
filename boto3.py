class _DummyClient:
    def __init__(self, service):
        self.service = service
    def __getattr__(self, name):
        def _call(*args, **kwargs):
            if name == "get_document_analysis":
                return {"Blocks": [], "JobStatus": "SUCCEEDED"}
            if name == "start_document_analysis":
                return {"JobId": "1"}
            return {}
        return _call

def client(service, **kwargs):
    return _DummyClient(service)

class _DummyTable:
    def __init__(self, name):
        self.name = name
    def scan(self):
        return {"Items": []}
    def put_item(self, Item=None):
        pass
    class batch_writer:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): pass
        def put_item(self, Item=None): pass

class _DummyResource:
    def __init__(self, service):
        self.service = service
    def Table(self, name):
        return _DummyTable(name)

def resource(service, **kwargs):
    return _DummyResource(service)
