from random import randrange
from locust import HttpUser, task

class QuickstartUser(HttpUser):
    @task
    def get_forecasts(self):
        lat = 56+randrange(20000)/10000 # 56 to 58, 4 decimal places
        lon = 20+randrange(80000)/10000 # 20 to 28, 4 decimal places
        self.client.get(f"/api/v1/forecast/cities?lat={lat}&lon={lon}&add_aurora=True&add_params=False")
