from .profile import FaultProfile
import random

class DemoFault(FaultProfile):
    def __init__(self):
        self.ts = self.get_timestamp()
        super().__init__("DemoMeterFault")

    def get_fault_up_until(self, upperBound):
        # 5% chance of updating timestamp
        if random.random() <= .05:
            self.ts = self.get_timestamp()
        return [{
            "name": self.name,
            "key": "Demo",
            "message": f"Got an energy fault at {self.ts}",
            "last_detected": self.ts,
            "details": {
                "meter": "main"
            }
        }]
