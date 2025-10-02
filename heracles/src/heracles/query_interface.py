from neo4j import GraphDatabase
import neo4j


class Neo4jWrapper:
    def __init__(
        self,
        db_uri,
        db_auth,
        db_name="neo4j",
        atomic_queries=True,
        print_profiles=False,
    ):
        self.driver = None
        self.session = None
        self.db_uri = db_uri
        self.db_auth = db_auth
        self.atomic_queries = atomic_queries
        self.print_profiles = print_profiles
        self.db_name = db_name

    def connect(self):
        self.driver = GraphDatabase.driver(self.db_uri, auth=self.db_auth)
        self.driver.verify_connectivity()
        if not self.atomic_queries:
            self.session = self.driver.session(database=self.db_name)

    def close(self):
        if self.session is not None:
            self.session.close()
        self.driver.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def execute(self, query, parameters=None, **kwargs):
        if self.atomic_queries:
            if self.print_profiles:
                query = "PROFILE\n" + query
            records, summary, etc = self.driver.execute_query(
                query, parameters=parameters, database_=self.db_name, **kwargs
            )

            if (
                self.print_profiles
                and summary is not None
                and summary.profile is not None
                and "args" in summary.profile
            ):
                print(summary.profile["args"]["string-representation"])

            return records, summary, etc
        else:
            ret = list(self.session.run(query, parameters=parameters, **kwargs))
            print("WARNING! Cannot print_profiles when atomic_queries is False")
            return list(ret), None, None

    def query(self, query):
        """A simplified query interface for use with LLMs"""
        _query = neo4j.Query(query, timeout=60)
        records, _, _ = self.driver.execute_query(_query, database_=self.db_name)
        return [r.data() for r in records]

    def query_with_notifications(self, query):
        _query = neo4j.Query(query, timeout=60)
        records, summary, _ = self.driver.execute_query(_query, database_=self.db_name)
        return [r.data() for r in records], summary.notifications
