from .profile import FaultProfile
import random

class VAVDemoFault(FaultProfile):
    def __init__(self, num_vavs=3):
        self.num_vavs = num_vavs
        super().__init__("VAVDemo")

    def get_fault_up_until(self, upperBound):
        res = []
        ts = self.get_timestamp()
        for i in range(self.num_vavs):
            if random.random() > .5:
                res.append({
                    "name": self.name,
                    "key": f"VAV-fault-{i}",
                    "message": "Something wrong with the VAV",
                    "last_detected": ts,
                    "details": {
                        "vav": f"VAV_{i}"
                    }
                })
        return res
