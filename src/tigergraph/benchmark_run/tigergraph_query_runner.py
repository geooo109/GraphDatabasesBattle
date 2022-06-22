class QueryRunner():
    def __init__(self):
        pass

    def query(self):
        pass

class TigergraphQueryRunner(QueryRunner):
    def __init__(self, conn):
        QueryRunner.__init__(self)
        self.conn = conn

    def query(self, query_name, parameters):
        return self.conn.runInstalledQuery(query_name, params=parameters)

if __name__ == "__main__":
    runner = TigergraphQueryRunner() 