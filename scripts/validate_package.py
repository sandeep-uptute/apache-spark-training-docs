"""Offline package integrity and notebook syntax validation; does not require PySpark."""
import ast, csv, json
from pathlib import Path
root=Path(__file__).resolve().parents[1]
required=[root/'training_lib.py', root/'data'/'transactions.csv',root/'data'/'customers.csv',root/'data'/'products.json',root/'data'/'products.tsv']
assert all(p.exists() for p in required)
assert len(list(csv.DictReader((root/'data'/'transactions.csv').open())))==58
for p in sorted((root/'notebooks').glob('*.ipynb')):
    book=json.loads(p.read_text())
    assert book.get('nbformat')==4
    for i,cell in enumerate(book['cells']):
        if cell['cell_type']=='code':
            source=''.join(cell['source'])
            try: ast.parse(source,filename=f'{p.name}:cell{i+1}')
            except SyntaxError as e: raise AssertionError(f'{p.name}, cell {i+1}: {e}')
    print('OK',p.name,len(book['cells']),'cells')
for p in (root/'notes').glob('*.md'):
    assert len(p.read_text())>1000, p
print('PASS: notebooks compile; data and notes found.')
