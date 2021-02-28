from gcp_secrets import *
import sqlalchemy
import os

class GCP:
    def __init__(self):
        db_config = {
        # [START cloud_sql_postgres_sqlalchemy_limit]
        # Pool size is the maximum number of permanent connections to keep.
        "pool_size": 5,
        # Temporarily exceeds the set pool_size if no connections are available.
        "max_overflow": 2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.
        # [END cloud_sql_postgres_sqlalchemy_limit]

        # [START cloud_sql_postgres_sqlalchemy_backoff]
        # SQLAlchemy automatically uses delays between failed connection attempts,
        # but provides no arguments for configuration.
        # [END cloud_sql_postgres_sqlalchemy_backoff]

        # [START cloud_sql_postgres_sqlalchemy_timeout]
        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        "pool_timeout": 30,  # 30 seconds
        # [END cloud_sql_postgres_sqlalchemy_timeout]

        # [START cloud_sql_postgres_sqlalchemy_lifetime]
        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # reestablished
        "pool_recycle": 1800,  # 30 minutes
        # [END cloud_sql_postgres_sqlalchemy_lifetime]
        }        
        # [START cloud_sql_postgres_sqlalchemy_create_socket]
        # Remember - storing secrets in plaintext is potentially unsafe. Consider using
        # something like https://cloud.google.com/secret-manager/docs/overview to help keep

        self.pool = sqlalchemy.create_engine(

            # Equivalent URL:
            # postgres+pg8000://<db_user>:<db_pass>@/<db_name>
            #                         ?unix_sock=<socket_path>/<cloud_sql_instance_name>/.s.PGSQL.5432
            sqlalchemy.engine.url.URL(
                drivername="postgresql+psycopg2",
                username=db_user,  # e.g. "my-database-user"
                password=db_password,  # e.g. "my-database-password"
                database=db_name,  # e.g. "my-database-name"
                query={
                    "unix_sock": "{}/{}".format(
                        db_socket_dir,  # e.g. "/cloudsql"
                        db_connection_name)  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
                }
            ),
            **db_config
        )
        # [END cloud_sql_postgres_sqlalchemy_create_socket]
        self.pool.dialect.description_encoding = None
        self.critic_ratings = 'critic_ratings'
        
            
        
    def get_review(self, movie, critic):
        '''
        Get review for a given movie for a given critic
        
        @param:
        movie - str identifier for movie
        critic - str critic id
        
        @return:
        rating - float score 0-100
        review - str review
        '''
        plan_name = 'get_review'
        try:
            with self.pool.connect() as conn:
                conn.execute(
                f"PREPARE {plan_name} (text, text) AS SELECT rating, review FROM {self.critic_ratings} WHERE movieid=$1 AND criticid=$2; \
                EXECUTE {plan_name}('{movie}', '{critic}');"
            )
                rows = conn.fetchall()
                conn.execute(f"DEALLOCATE {plan_name}")
            
            return (float(rows[0][0]), rows[0][1])
        except Exception as e:
            print("ERROR [gcp get review]:", e)            
            return None


    def get_critic(self, critic):
        '''
        Get all reviews for a critic
        
        @param:
        critic - str critic id

        @return:
        reviews - list of dicts containing movie names and ratings [{moveid, rating, review}]
        '''
        plan_name = 'get_critic'
        try:
            with self.pool.connect() as conn:
                conn.execute(
                f"PREPARE {plan_name} (text) AS SELECT movieid, rating, review FROM {self.critic_ratings} WHERE criticid=$1; \
                EXECUTE {plan_name}('{critic}');"
            )
                rows = conn.fetchall()
                conn.execute(f"DEALLOCATE {plan_name}")
            return [{'id': item[0], 'rating': float(item[1]), 'review': item[2]} for item in rows]
        except Exception as e:
            print("ERROR [gcp get critic]:", e)
            return None
