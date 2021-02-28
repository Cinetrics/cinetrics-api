import psycopg2
from cockroach_login import *
import psycopg2


class Cockroach:
    # TODO async
    def __init__(self):
        self.user_accounts = "user_accounts"
        self.user_preferences = "user_preferences"
        self.critic_ratings = "critic_ratings"
        self.critic_metadata = "critic_metadata"

        self.conn = psycopg2.connect(
            f"postgres://{username}:{password}@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/sunny-goat-993.cinetrics?sslmode=verify-full&sslrootcert={cockroach_path}/certs/cc-ca.crt", 
        )

    def add_account(self, email, password_hash):
        """
        Add given user to database

        @param:
        email - str email
        password_hash - str bcrypt hash

        @return:
        bool - success
        """
        plan_name = "add_user"
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DEALLOCATE ALL")
                cur.execute(
                    f"PREPARE {plan_name} (text, text) AS INSERT INTO {self.user_accounts} VALUES($1, $2); \
                EXECUTE {plan_name} ('{email}', '{password_hash}');"
                )
                cur.execute(f"DEALLOCATE {plan_name}")
            self.conn.commit()
            return bool(cur.statusmessage[-1])
        except Exception as e:
            print("DB ERROR [add acct]: ", e)
            self.conn.commit()
            return False

    def get_auth(self, email):
        """
        Get password hash for user login

        @param:
        email - str email for user login

        @return:
        hash - str user password hash, None if user doesn't exist
        """
        plan_name = "get_auth"
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DEALLOCATE ALL")
                cur.execute(
                    f"PREPARE {plan_name} (text) AS SELECT hash FROM {self.user_accounts} WHERE username=$1; \
                EXECUTE {plan_name}('{email}');"
                )
                rows = cur.fetchall()
                cur.execute(f"DEALLOCATE {plan_name}")
            self.conn.commit()
            if len(rows) > 1:
                raise Exception("Duplicate users in database")
            return rows[0][0]
        except Exception as e:
            print("DB ERROR get auth:", e)
            self.conn.commit()
            return None

    def send_rating(self, email, movie, rating):
        """
        Add rating to user rating table

        @param:
        email - str user email
        movie - str movie url
        rating - float 0-100

        @return:
        None
        """
        plan_name = "send_rating"
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DEALLOCATE ALL")
                cur.execute(
                    f"PREPARE {plan_name} (text, text, decimal) AS INSERT INTO {self.user_preferences} VALUES($1, $2, $3); \
                EXECUTE {plan_name}('{email}', '{movie}', {rating});"
                )
                cur.execute(f"DEALLOCATE {plan_name}")
            self.conn.commit()

            return bool(cur.statusmessage[-1])
        except Exception as e:
            print("DB ERROR send rating: ", e)
            self.conn.commit()
            return False

    def pull_ratings(self, email):
        """
        Get all ratings for a given user

        @param:
        email - str email for user

        @return:
        ratings - list of dicts containing movie names and ratings [{moveid, rating}]
        """
        plan_name = "pull_ratings"
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"PREPARE {plan_name} (text) AS SELECT movieid, rating FROM {self.user_preferences} WHERE username=$1; \
                EXECUTE {plan_name}('{email}');"
                )
                rows = cur.fetchall()
                cur.execute(f"DEALLOCATE {plan_name}")
                self.conn.commit()
            return [{"id": int(item[0]), "rating": float(item[1])} for item in rows]
        except Exception as e:
            print("DB ERROR [pull ratings]: ", e)
            self.conn.commit()
            return None

    def del_ratings(self, email):
        """
        Delete all user reviews

        @param:
        email - str email for user

        @return:
        None
        """
        plan_name = "del_ratings"
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"PREPARE {plan_name} (text) AS DELETE FROM {self.user_preferences} WHERE username=$1; \
                EXECUTE {plan_name}('{email}');"
                )
                cur.execute(f"DEALLOCATE {plan_name}")
                self.conn.commit()
        except Exception as e:
            print("DB ERROR [del ratings]: ", e)
            self.conn.commit()
