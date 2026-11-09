#!/usr/bin/env python3
"""Deterministic offline training data generator: Python standard library only."""
from pathlib import Path
import csv, json, random
from decimal import Decimal
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
DATA.mkdir(exist_ok=True)

products = [
    ('P001','Laptop Stand','Accessories','1499.00'),
    ('P002','USB-C Hub','Accessories','2299.00'),
    ('P003','Wireless Mouse','Accessories','799.00'),
    ('P004','Mechanical Keyboard','Accessories','3499.00'),
    ('P005','Headphones','Audio','2499.00'),
    ('P006','Bluetooth Speaker','Audio','2999.00'),
    ('P007','Webcam','Electronics','3999.00'),
    ('P008','Monitor','Electronics','12999.00'),
    ('P009','Notebook','Stationery','149.00'),
    ('P010','Desk Organizer','Stationery','599.00'),
    ('P011','Portable SSD','Electronics','6499.00'),
    ('P012','USB Cable','Accessories','399.00'),
]
customers = [
    ('C001','Aditi','Pune'),('C002','Farhan','Mumbai'),('C003','Neha','Bengaluru'),
    ('C004','Rohan','Delhi'),('C005','Isha','Pune'),('C006','Kabir','Hyderabad'),
    ('C007','Tara','Chennai'),('C008','Nikhil','Mumbai'),('C009','Zoya','Delhi'),
    ('C010','Manav','Bengaluru'),('C011','Sana','Ahmedabad'),('C012','Pooja','Pune'),
]
rows = []
random.seed(117)
for i in range(48):
    p = products[(i * 7 + 2) % len(products)]
    c = customers[(i * 5 + 1) % len(customers)]
    status = ('COMPLETED' if i % 8 not in (1,6) else ('CANCELLED' if i % 8 == 1 else 'RETURNED'))
    qty = str((i % 3) + 1)
    rows.append({
        'transaction_id':f'T{1001+i}',
        'transaction_date':f'2026-10-{1+(i%7):02d}',
        'customer_id':c[0], 'product_id':p[0], 'quantity':qty,
        'unit_price':p[3], 'status':status,
    })
# Inject one *identical* duplicate and representative data-quality exceptions.
rows.append(dict(rows[4]))
rows.extend([
    dict(transaction_id='T1049',transaction_date='2026-10-05',customer_id='C001',product_id='P001',quantity='',unit_price='1499.00',status='COMPLETED'),
    dict(transaction_id='T1050',transaction_date='2026-10-05',customer_id='C002',product_id='P002',quantity='not-a-number',unit_price='2299.00',status='COMPLETED'),
    dict(transaction_id='T1051',transaction_date='2026-10-06',customer_id='C003',product_id='P003',quantity='2',unit_price='',status='COMPLETED'),
    dict(transaction_id='T1052',transaction_date='wrong-date',customer_id='C004',product_id='P004',quantity='1',unit_price='3499.00',status='COMPLETED'),
    dict(transaction_id='T1053',transaction_date='2026-10-06',customer_id='C005',product_id='P999',quantity='1',unit_price='1000.00',status='COMPLETED'),
    dict(transaction_id='T1054',transaction_date='2026-10-07',customer_id='C999',product_id='P005',quantity='1',unit_price='2499.00',status='COMPLETED'),
    dict(transaction_id='T1055',transaction_date='2026-10-07',customer_id='C006',product_id='P006',quantity='-1',unit_price='2999.00',status='COMPLETED'),
    dict(transaction_id='T1056',transaction_date='2026-10-07',customer_id='C007',product_id='',quantity='1',unit_price='500.00',status='COMPLETED'),
    dict(transaction_id='T1057',transaction_date='2026-10-07',customer_id='',product_id='P007',quantity='1',unit_price='3999.00',status='COMPLETED'),
])

with (DATA/'transactions.csv').open('w',newline='',encoding='utf8') as f:
    w=csv.DictWriter(f,fieldnames=['transaction_id','transaction_date','customer_id','product_id','quantity','unit_price','status']); w.writeheader();w.writerows(rows)
with (DATA/'customers.csv').open('w',newline='',encoding='utf8') as f:
    w=csv.writer(f);w.writerow(['customer_id','customer_name','city']);w.writerows(customers)
with (DATA/'products.tsv').open('w',newline='',encoding='utf8') as f:
    w=csv.writer(f,delimiter='\t');w.writerow(['product_id','product_name','category','list_price']);w.writerows(products)
with (DATA/'products.json').open('w',encoding='utf8') as f:
    for pid,name,cat,price in products:
        f.write(json.dumps({'product_id':pid,'product_name':name,'category':cat,'list_price':float(price)})+'\n')

prods={x[0]:x for x in products}; custids={x[0] for x in customers}
unique={r['transaction_id']:r for r in rows}
clean=[]
for r in unique.values():
    try:
        if not r['transaction_date'].startswith('2026-10-') or len(r['transaction_date'])!=10: continue
        q=int(r['quantity']);p=Decimal(r['unit_price'])
        if q<=0 or p<0 or not r['customer_id'] or not r['product_id']: continue
        clean.append(r)
    except (ValueError,ArithmeticError):continue
completed=[r for r in clean if r['status']=='COMPLETED' and r['product_id'] in prods and r['customer_id'] in custids]
category=defaultdict(lambda:{'revenue':Decimal('0'),'orders':0})
for r in completed:
    d=category[prods[r['product_id']][2]]
    d['revenue']+=Decimal(r['unit_price'])*int(r['quantity'])
    d['orders']+=1
manifest={
 'raw_rows':len(rows),'unique_transaction_rows':len(unique),'clean_rows':len(clean),
 'rejected_for_type_or_missing_value':len(unique)-len(clean),
 'completed_valid_reference_rows':len(completed),
 'total_completed_revenue':str(sum((Decimal(r['unit_price'])*int(r['quantity']) for r in completed),Decimal('0')).quantize(Decimal('0.01'))),
 'category_summary':{k:{'revenue':str(v['revenue'].quantize(Decimal('0.01'))),'orders':v['orders']} for k,v in sorted(category.items())},
 'unmatched_product_ids':['P999'],'unmatched_customer_ids':['C999']
}
(ROOT/'solutions'/'expected_metrics.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf8')
print(manifest)
