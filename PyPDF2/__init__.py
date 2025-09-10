class _Page:
    def merge_page(self, other):
        pass

class PdfReader:
    def __init__(self, f):
        self.pages = [_Page()]

class PdfWriter:
    def __init__(self):
        self.pages = []
    def add_page(self, page):
        self.pages.append(page)
    def write(self, f):
        if hasattr(f, 'write'):
            f.write(b'%PDF-stub')
    def encrypt(self, pwd):
        pass
