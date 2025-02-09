"""
utils_generate_rafting_data.py

Utility for generating synthetic rafting feedback data.

This script generates and saves 170 rafting reviews (150 positive, 20 negative)
from Memorial Day to Labor Day 2024.

Usage:
    from utils.utils_generate_rafting_data import generate_rafting_feedback
    data_file = generate_rafting_feedback()
"""

from datetime import datetime, timedelta
import random
import json
import uuid
import pathlib

# Define rafting guides
GUIDES = ["Jake", "Samantha", "Carlos", "Emily", "Tyler", "Ava", "Liam", "Sophia", "Mason", "Olivia"]

# Define possible rafting trip times
TRIP_TYPES = ["Half Day", "Full Day"]

# Define the date range (Memorial Day 2024 to Labor Day 2024)
MEMORIAL_DAY_2024 = datetime(2024, 5, 27)
LABOR_DAY_2024 = datetime(2024, 9, 2)
DATE_RANGE = (LABOR_DAY_2024 - MEMORIAL_DAY_2024).days

# Define positive and negative customer comments
# Define positive customer comments
POSITIVE_COMMENTS = [
    "An absolutely thrilling experience! Would do it again.",
    "Our guide was fantastic, made us feel safe the entire time.",
    "The rapids were intense! Such an adrenaline rush.",
    "A great weekend adventure, I highly recommend it.",
    "Loved the scenery, the river was beautiful.",
    "Best weekend trip I've had in years!",
    "We got completely soaked, but it was worth it!",
    "The guide was so knowledgeable, learned a lot about the river.",
    "Perfect mix of excitement and relaxation.",
    "Had a great time with family, will be back next year.",
    "The guides were professional and super fun!",
    "Everything was well-organized and seamless.",
    "Loved the challenge of the rapids, definitely coming back.",
    "The equipment was top-notch and well-maintained.",
    "One of the best outdoor adventures I’ve ever had!",
    "Great for both beginners and experienced rafters.",
    "The lunch provided was delicious and fresh.",
    "The guides really knew their stuff and made us feel comfortable.",
    "An unforgettable experience, can’t wait to book again!",
    "The whole trip was a perfect balance of fun and excitement.",
    "We saw so much wildlife along the river, incredible!",
    "Would highly recommend this for thrill-seekers.",
    "Great bonding experience for our group.",
    "Such a peaceful yet exhilarating adventure.",
    "Loved the team spirit our guide encouraged.",
    "The water was just perfect for rafting!",
    "Felt completely safe the entire time.",
    "A must-do experience for nature lovers!",
    "We laughed so much! The guides were hilarious.",
    "Amazing views, felt like a scene from a movie.",
    "A bucket-list experience checked off!",
    "The whole experience exceeded my expectations.",
    "Loved the rush of navigating the rapids.",
    "The trip was well-paced and enjoyable for all skill levels.",
    "Even the calmer sections of the river were fun and engaging.",
    "The pre-trip instructions were thorough and helpful.",
    "A great way to escape the city and enjoy nature.",
    "Met some awesome people on this trip!",
    "So much fun! Worth every penny.",
    "The sunset over the river was breathtaking.",
    "We felt like pros thanks to the expert guidance.",
    "Even my kids had an amazing time!",
    "Great for corporate team-building activities.",
    "The guides kept us entertained the whole time.",
    "Loved the thrill of hitting the bigger waves!",
    "Perfect mix of adventure and relaxation.",
    "I can’t stop talking about this trip!",
    "Saw an eagle soaring above us—what a moment!",
    "Everything was well-planned and executed smoothly.",
    "Already planning my next trip!",
    "So much energy and enthusiasm from the guides.",
    "This trip turned me into a rafting enthusiast!",
    "Nothing beats the feeling of conquering a tough rapid!"
]

# Define negative customer comments
NEGATIVE_COMMENTS = [
    "The water was too rough, not what I expected.",
    "Our guide seemed uninterested and didn't engage much.",
    "The equipment was old and worn out.",
    "Too many people on the raft, felt overcrowded.",
    "Not enough instructions given before the trip.",
    "The rapids were too intense for beginners.",
    "Felt unsafe at times, the guide wasn't very reassuring.",
    "Too expensive for what it was.",
    "Expected a longer trip, but it felt too short.",
    "The campsite was poorly maintained.",
    "The guide was not engaging. Not what I expected.",
    "The food provided was terrible and lacked options.",
    "The bus ride to the starting point was uncomfortably long.",
    "We had to wait too long before getting started.",
    "The restroom facilities were dirty and lacked supplies.",
    "The safety gear provided didn’t fit properly.",
    "There was too much waiting around, not enough action.",
    "Poor communication about what to bring and expect.",
    "The experience did not match the description on the website.",
    "The staff was rude and unhelpful when we asked questions.",
    "The water was freezing, and we weren’t warned beforehand.",
    "The pictures on the website were misleading.",
    "Too many hidden fees, ended up costing way more than expected.",
    "The wetsuits provided were smelly and damp.",
    "We felt rushed through the entire experience.",
    "The weather was bad, but no alternatives were offered.",
    "Some sections felt too slow and boring.",
    "We were split from our group without warning.",
    "The booking process was confusing and frustrating.",
    "The guides seemed more focused on their own fun than ours.",
    "The shuttle service was late, causing delays."
]

def generate_rafting_feedback(output_file="data/all_rafting_remarks.json"):
    """
    Generate and save rafting customer feedback.

    Args:
        output_file (str): Path where the JSON file will be saved.

    Returns:
        pathlib.Path: The path to the generated JSON file.
    """
    data_folder = pathlib.Path(output_file).parent
    data_folder.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
    data_file = pathlib.Path(output_file)

    # Generate 150 positive and 20 negative comments
    customer_remarks = [
        {
            "comment": random.choice(POSITIVE_COMMENTS),
            "guide": random.choice(GUIDES),
            "uuid": str(uuid.uuid4()),
            "date": (MEMORIAL_DAY_2024 + timedelta(days=random.randint(0, DATE_RANGE))).strftime("%Y-%m-%d"),
            "trip_type": random.choice(TRIP_TYPES),
            "timestamp": datetime.utcnow().isoformat(),
            "is_negative": False,
        }
        for _ in range(150)
    ] + [
        {
            "comment": random.choice(NEGATIVE_COMMENTS),
            "guide": random.choice(GUIDES),
            "uuid": str(uuid.uuid4()),
            "date": (MEMORIAL_DAY_2024 + timedelta(days=random.randint(0, DATE_RANGE))).strftime("%Y-%m-%d"),
            "trip_type": random.choice(TRIP_TYPES),
            "timestamp": datetime.utcnow().isoformat(),
            "is_negative": True,
        }
        for _ in range(20)
    ]

    # Save to a JSON file
    with open(data_file, "w") as file:
        json.dump(customer_remarks, file, indent=4)

    return data_file  # Return the path for confirmation

# Example usage:
if __name__ == "__main__":
    generated_file = generate_rafting_feedback()
    print(f"Generated rafting feedback file: {generated_file}")
