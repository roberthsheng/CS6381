###############################################
#
# Purpose: Topic selection and generation utilities
#
###############################################

import os
import random
import time

class TopicSelector:
    """Utility class for managing topics in the pub/sub system"""

    def __init__(self):
        # Pre-defined list of possible topics
        self.topics = [
            "Weather",
            "News",
            "Sports",
            "Traffic",
            "Stock",
            "Healthcare",
            "Technology",
            "Entertainment",
            "Science"
        ]

        # Define common topic groups to ensure overlap
        self.topic_groups = {
            "group1": ["Weather", "Sports"],
            "group2": ["News", "Technology"],
            "group3": ["Stock", "Healthcare"],
            "group4": ["Traffic", "Entertainment"]
        }

        # Dictionary to store value ranges for each topic
        self.value_ranges = {
            "Weather": (0, 100),    # Temperature in Fahrenheit
            "News": (1, 5),         # Priority level
            "Sports": (0, 100),     # Score
            "Traffic": (0, 60),     # Speed in mph
            "Stock": (1, 1000),     # Price in dollars
            "Healthcare": (1, 10),   # Severity level
            "Technology": (1, 5),    # Version number
            "Entertainment": (1, 5), # Rating
            "Science": (0, 1000)     # Measurement value
        }

    def interest(self, num_topics):
        """Select a number of topics to subscribe to or publish"""
        if num_topics > len(self.topics):
            raise ValueError(f"Requested topics ({num_topics}) exceeds available topics ({len(self.topics)})")
        
        # Use seeded random to ensure some overlap
        random.seed(time.time())
        
        # First pick a topic group to ensure overlap
        group = list(self.topic_groups.values())[1 % len(self.topic_groups)]
        
        # If we need more topics than the group provides, add some random ones
        result = group[:num_topics]
        if len(result) < num_topics:
            remaining = random.sample([t for t in self.topics if t not in result], 
                                   num_topics - len(result))
            result.extend(remaining)
            
        return result[:num_topics]

    def gen_publication(self, topic):
        """Generate a value for a given topic"""
        if topic not in self.value_ranges:
            raise ValueError(f"Unknown topic: {topic}")
        
        min_val, max_val = self.value_ranges[topic]
        value = random.uniform(min_val, max_val)
        
        # Format based on topic type
        if topic in ["Weather", "Traffic", "Stock", "Science"]:
            return f"{value:.2f}"
        else:
            return f"{int(value)}"
