# Credit: SpotifyEng @ https://gist.githubusercontent.com/ShopifyEng/d2eed58df952677562eb54fe36ebaabb/raw/f4b374e540821f086d22fbefd36168811be19239/random_airflow_schedule.py

from hashlib import md5
from random import randint, seed

def compute_schedule(dag_id, interval):

  # create deterministic randomness by seeding PRNG with a hash of the table name:
  seed(md5(dag_id.encode()).hexdigest())

  if interval.endswith("h"):
    val = int(interval[:-1])
    if 24 % val != 0:
      raise ValueError("Must use a number which evenly divides 24.")
    offset = randint(0, val-1)
    minutes = randint(0, 59)
    return f"{minutes} {offset}/{val} * * *"

  elif interval.endswith("m"):
    val = int(interval[:-1])
    if 60 % val != 0:
      raise ValueError("Minutes must use a number which evenly divides 60.")
    offset = randint(0, val-1)
    return f"{offset}/{val} * * * *"

  elif interval == "1d":
    return f"{randint(0, 59)} {randint(0, 23)} * * *"

  raise ValueError("Interval must be (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)m, (1, 2, 3, 4, 6, 12)h or 1d")