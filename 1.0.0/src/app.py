from walkoff_app_sdk.app_base import AppBase
import psycopg
import json

class PostgreSQL(AppBase):
    """
    An example of a Walkoff App.
    Inherit from the AppBase class to have Redis, logging, and console logging set up behind the scenes.
    """
    __version__ = "1.0.0"
    app_name = "PostgreSQL"  # this needs to match "name" in api.yaml

    def __init__(self, redis, logger, console_logger=None):
        """
        Each app should have this __init__ to set up Redis and logging.
        :param redis:
        :param logger:
        """
        super().__init__(redis, logger, console_logger)
    async def query(self,dbname,user,host,password,query,output):
        with psycopg.connect("dbname="+dbname+" user="+user+" host="+host+" password="+password) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
            try:
                conn.commit()
                if output:
                    cur.fetchone()
                    conn.commit()
                    return json.dumps({"status":"successfull","results":cur.fetchall()})
                else:
                    return json.dumps({"status":"successfull"})
                print("Successfull")
            except BaseException:
                conn.rollback()
                print("Error")
                return({"status":"error"})
            finally:
                conn.close()
                print("Close")

if __name__ == "__main__":
    PostgreSQL.run()
