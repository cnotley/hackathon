class Cell:
    def __init__(self, value=None):
        self.value = value
        self.fill = None

class Worksheet:
    def __init__(self, title="Sheet1"):
        self.title = title
        self._cells = {}

    def append(self, row):
        r = self.max_row + 1
        for idx, v in enumerate(row, start=1):
            self._cells[(r, idx)] = Cell(v)

    def cell(self, row, column):
        key = (row, column)
        if key not in self._cells:
            self._cells[key] = Cell()
        return self._cells[key]

    def _coord(self, key):
        col = 0
        row = ""
        for ch in key:
            if ch.isalpha():
                col = col * 26 + (ord(ch.upper()) - 64)
            elif ch.isdigit():
                row += ch
        return int(row or 1), col

    def __getitem__(self, key):
        r, c = self._coord(key)
        return self.cell(r, c)

    def __setitem__(self, key, value):
        r, c = self._coord(key)
        self.cell(r, c).value = value

    @property
    def max_row(self):
        if not self._cells:
            return 0
        return max(r for (r, _) in self._cells.keys())

class Workbook:
    def __init__(self):
        self._sheets = {}
        self.active = self.create_sheet("Sheet1")

    def create_sheet(self, title="Sheet"):
        ws = Worksheet(title)
        self._sheets[title] = ws
        return ws

    def __getitem__(self, name):
        return self._sheets[name]

    def save(self, path):
        import json
        data = {title: {f"{r},{c}": cell.value for (r,c), cell in ws._cells.items()} for title, ws in self._sheets.items()}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)


def load_workbook(file_obj):
    import json
    if hasattr(file_obj, "read"):
        content = file_obj.read()
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        data = json.loads(content)
    else:
        with open(file_obj, "r", encoding="utf-8") as f:
            data = json.load(f)
    wb = Workbook()
    wb._sheets = {}
    for title, cells in data.items():
        ws = Worksheet(title)
        for key, value in cells.items():
            r, c = map(int, key.split(","))
            ws._cells[(r, c)] = Cell(value)
        wb._sheets[title] = ws
    wb.active = next(iter(wb._sheets.values()))
    return wb
