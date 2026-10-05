import random
from datetime import datetime, timedelta, timezone
from psycopg import sql
from .storage import pg, catalog

def generate(count=1000, customers=100, seed=42):
    rng = random.Random(seed)
    schema = catalog()
    with pg() as c:
        with c.transaction():
            for i in range(1,customers+1):
                c.execute("INSERT INTO sales.customers(id,name,segment) VALUES (%s,%s,%s) ON CONFLICT(id) DO UPDATE SET updated_at=now()",
                    (i,f'Customer {i}',rng.choice(['retail','business'])))
            start = c.execute('SELECT coalesce(max(id),0)+1 FROM sales.orders').fetchone()[0]
            fields = ['id','customer_id']
            # During expand phase populate both old and new names.
            fields += [x for x in ('amount','total_amount') if x in schema['orders']]
            fields += ['currency','status','ordered_at']
            if 'country' in schema['orders']: fields.append('country')
            stmt = sql.SQL('INSERT INTO sales.orders ({}) VALUES ({})').format(
                sql.SQL(',').join(map(sql.Identifier,fields)),sql.SQL(',').join(sql.Placeholder() for _ in fields))
            for i in range(start,start+count):
                amount = rng.randint(100,100000)
                ordered = datetime.now(timezone.utc)-timedelta(days=rng.randint(0,90))
                row = {'id':i,'customer_id':rng.randint(1,customers),'amount':amount,'total_amount':amount,
                    'currency':'PLN','status':'created','ordered_at':ordered,'country':rng.choice(['PL','DE','FR','GB','US'])}
                for f in ('amount','total_amount'):
                    if schema['orders'].get(f,{}).get('type') == 'text': row[f] = str(amount)
                c.execute(stmt,[row[f] for f in fields])
    return {'orders':count,'first_id':start,'last_id':start+count-1,'customers':customers}

def mutate(order_id, status=None, delete=False):
    with pg() as c:
        if delete:
            return c.execute('DELETE FROM sales.orders WHERE id=%s',(order_id,)).rowcount
        return c.execute('UPDATE sales.orders SET status=%s,updated_at=now() WHERE id=%s',(status or 'paid',order_id)).rowcount
