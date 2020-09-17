from datetime import datetime

class FaultProfile:
    def __init__(self, name):
        self.name = name

    def get_timestamp(self):
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")

    def get_fault_up_until(self, upperBound):
        """
        Returns list of fault statuses considering data *up until* the Datetime
        given by 'upperBound'. By artificially increasing upperBound, we can
        emulate historical faults as if they were real-time or even accelerated
        time.

        Each fault status looks like the following.
        "key" must be a globally unique string
        {
            "name": self.name,
            "key": "key that is unique to this fault condition being true",
            "message": "message here",
            "last_detected": datetime,
            "details": {
                << additional k-v pairs >>
            }
        }
        """
        raise NotImplemented("not implemented")
