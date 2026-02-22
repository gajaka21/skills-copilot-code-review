"""
MongoDB database configuration and setup for Mergington High School API
"""

from copy import deepcopy
from pymongo import MongoClient
from argon2 import PasswordHasher, exceptions as argon2_exceptions


class _UpdateResult:
    def __init__(self, modified_count: int):
        self.modified_count = modified_count


class InMemoryCollection:
    def __init__(self):
        self._documents = {}

    def count_documents(self, query):
        if query:
            return sum(1 for _ in self.find(query))
        return len(self._documents)

    def insert_one(self, document):
        doc = deepcopy(document)
        self._documents[doc["_id"]] = doc

    def find_one(self, query):
        """
        Return a single document matching the query.

        Supports both direct '_id' lookups and more general queries,
        using the same matching logic as the 'find' method.
        """
        if query is None:
            query = {}

        # Fast path for direct '_id' lookups
        doc_id = query.get("_id")
        if doc_id is not None:
            doc = self._documents.get(doc_id)
            return deepcopy(doc) if doc else None

        # General query path: return the first document that matches
        for document in self._documents.values():
            if self._matches(document, query):
                return deepcopy(document)

        return None
    def _matches(self, document, query):
        if not query:
            return True

        for key, value in query.items():
            if key == "_id":
                if document.get("_id") != value:
                    return False
            elif key == "schedule_details.days":
                days = document.get("schedule_details", {}).get("days", [])
                required_days = value.get("$in", [])
                if not any(day in days for day in required_days):
                    return False
            elif key == "schedule_details.start_time":
                start_time = document.get("schedule_details", {}).get("start_time", "")
                if start_time < value.get("$gte", ""):
                    return False
            elif key == "schedule_details.end_time":
                end_time = document.get("schedule_details", {}).get("end_time", "")
                if end_time > value.get("$lte", ""):
                    return False
        return True

    def find(self, query=None):
        for document in self._documents.values():
            if self._matches(document, query or {}):
                yield deepcopy(document)

    def update_one(self, query, update):
        doc_id = query.get("_id")
        if doc_id is None or doc_id not in self._documents:
            return _UpdateResult(0)

        document = self._documents[doc_id]
        modified = False

        if "$push" in update:
            push_modified = False
            for field, value in update["$push"].items():
                current = document.setdefault(field, [])
                current.append(value)
                push_modified = True
            modified = modified or push_modified

        if "$pull" in update:
            pull_modified = False
            for field, value in update["$pull"].items():
                current = document.get(field, [])
                if isinstance(current, list):
                    original_len = len(current)
                    # Remove all occurrences of the value
                    current[:] = [item for item in current if item != value]
                    if len(current) != original_len:
                        pull_modified = True
            modified = modified or pull_modified

        return _UpdateResult(1 if modified else 0)

    def aggregate(self, pipeline):
        if pipeline == [
            {"$unwind": "$schedule_details.days"},
            {"$group": {"_id": "$schedule_details.days"}},
            {"$sort": {"_id": 1}},
        ]:
            days = set()
            for document in self._documents.values():
                days.update(document.get("schedule_details", {}).get("days", []))
            for day in sorted(days):
                yield {"_id": day}


def _create_database_collections():
    try:
        mongo_client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1500)
        mongo_client.admin.command("ping")
        mongo_db = mongo_client["mergington_high"]
        return mongo_client, mongo_db["activities"], mongo_db["teachers"]
    except Exception:
        return None, InMemoryCollection(), InMemoryCollection()


client, activities_collection, teachers_collection = _create_database_collections()

# Methods


def hash_password(password):
    """Hash password using Argon2"""
    ph = PasswordHasher()
    return ph.hash(password)


def verify_password(hashed_password: str, plain_password: str) -> bool:
    """Verify a plain password against an Argon2 hashed password.

    Returns True when the password matches, False otherwise.
    """
    ph = PasswordHasher()
    try:
        ph.verify(hashed_password, plain_password)
        return True
    except argon2_exceptions.VerifyMismatchError:
        return False
    except Exception:
        # For any other exception (e.g., invalid hash), treat as non-match
        return False


def init_database():
    """Initialize database if empty"""

    # Initialize activities if empty
    if activities_collection.count_documents({}) == 0:
        for name, details in initial_activities.items():
            activities_collection.insert_one({"_id": name, **details})

    # Initialize teacher accounts if empty
    if teachers_collection.count_documents({}) == 0:
        for teacher in initial_teachers:
            teachers_collection.insert_one(
                {"_id": teacher["username"], **teacher})


# Initial database if empty
initial_activities = {
    "Chess Club": {
        "description": "Learn strategies and compete in chess tournaments",
        "schedule": "Mondays and Fridays, 3:15 PM - 4:45 PM",
        "schedule_details": {
            "days": ["Monday", "Friday"],
            "start_time": "15:15",
            "end_time": "16:45"
        },
        "max_participants": 12,
        "participants": ["michael@mergington.edu", "daniel@mergington.edu"]
    },
    "Programming Class": {
        "description": "Learn programming fundamentals and build software projects",
        "schedule": "Tuesdays and Thursdays, 7:00 AM - 8:00 AM",
        "schedule_details": {
            "days": ["Tuesday", "Thursday"],
            "start_time": "07:00",
            "end_time": "08:00"
        },
        "max_participants": 20,
        "participants": ["emma@mergington.edu", "sophia@mergington.edu"]
    },
    "Morning Fitness": {
        "description": "Early morning physical training and exercises",
        "schedule": "Mondays, Wednesdays, Fridays, 6:30 AM - 7:45 AM",
        "schedule_details": {
            "days": ["Monday", "Wednesday", "Friday"],
            "start_time": "06:30",
            "end_time": "07:45"
        },
        "max_participants": 30,
        "participants": ["john@mergington.edu", "olivia@mergington.edu"]
    },
    "Soccer Team": {
        "description": "Join the school soccer team and compete in matches",
        "schedule": "Tuesdays and Thursdays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Tuesday", "Thursday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 22,
        "participants": ["liam@mergington.edu", "noah@mergington.edu"]
    },
    "Basketball Team": {
        "description": "Practice and compete in basketball tournaments",
        "schedule": "Wednesdays and Fridays, 3:15 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Wednesday", "Friday"],
            "start_time": "15:15",
            "end_time": "17:00"
        },
        "max_participants": 15,
        "participants": ["ava@mergington.edu", "mia@mergington.edu"]
    },
    "Art Club": {
        "description": "Explore various art techniques and create masterpieces",
        "schedule": "Thursdays, 3:15 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Thursday"],
            "start_time": "15:15",
            "end_time": "17:00"
        },
        "max_participants": 15,
        "participants": ["amelia@mergington.edu", "harper@mergington.edu"]
    },
    "Drama Club": {
        "description": "Act, direct, and produce plays and performances",
        "schedule": "Mondays and Wednesdays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Monday", "Wednesday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 20,
        "participants": ["ella@mergington.edu", "scarlett@mergington.edu"]
    },
    "Math Club": {
        "description": "Solve challenging problems and prepare for math competitions",
        "schedule": "Tuesdays, 7:15 AM - 8:00 AM",
        "schedule_details": {
            "days": ["Tuesday"],
            "start_time": "07:15",
            "end_time": "08:00"
        },
        "max_participants": 10,
        "participants": ["james@mergington.edu", "benjamin@mergington.edu"]
    },
    "Debate Team": {
        "description": "Develop public speaking and argumentation skills",
        "schedule": "Fridays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Friday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 12,
        "participants": ["charlotte@mergington.edu", "amelia@mergington.edu"]
    },
    "Weekend Robotics Workshop": {
        "description": "Build and program robots in our state-of-the-art workshop",
        "schedule": "Saturdays, 10:00 AM - 2:00 PM",
        "schedule_details": {
            "days": ["Saturday"],
            "start_time": "10:00",
            "end_time": "14:00"
        },
        "max_participants": 15,
        "participants": ["ethan@mergington.edu", "oliver@mergington.edu"]
    },
    "Science Olympiad": {
        "description": "Weekend science competition preparation for regional and state events",
        "schedule": "Saturdays, 1:00 PM - 4:00 PM",
        "schedule_details": {
            "days": ["Saturday"],
            "start_time": "13:00",
            "end_time": "16:00"
        },
        "max_participants": 18,
        "participants": ["isabella@mergington.edu", "lucas@mergington.edu"]
    },
    "Sunday Chess Tournament": {
        "description": "Weekly tournament for serious chess players with rankings",
        "schedule": "Sundays, 2:00 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Sunday"],
            "start_time": "14:00",
            "end_time": "17:00"
        },
        "max_participants": 16,
        "participants": ["william@mergington.edu", "jacob@mergington.edu"]
    }
}

initial_teachers = [
    {
        "username": "mrodriguez",
        "display_name": "Ms. Rodriguez",
        "password": hash_password("art123"),
        "role": "teacher"
    },
    {
        "username": "mchen",
        "display_name": "Mr. Chen",
        "password": hash_password("chess456"),
        "role": "teacher"
    },
    {
        "username": "principal",
        "display_name": "Principal Martinez",
        "password": hash_password("admin789"),
        "role": "admin"
    }
]
