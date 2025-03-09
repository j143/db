import sqlparse

def parse_query(query):
    parsed = sqlparse.parse(query)[0]
    print(parsed.tokens)
    tokens = [token.value for token in parsed.tokens if token.ttype is None]
    print("Tokens: ", tokens)

#query example
query = "SELECT name, age FROM customers WHERE age > 30 LIMIT 5"
parse_query(query)
