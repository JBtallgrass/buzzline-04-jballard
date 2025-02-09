"""
utils_generate_rafting_data.py

Utility for generating synthetic rafting feedback data.

This script generates and saves rafting reviews (200 positive, 50 negative) 
from Memorial Day to Labor Day 2024, featuring 20 unique guides and a mix of half-day and full-day trips.

Usage:
    from utils.utils_generate_rafting_data import generate_rafting_feedback
    data_file = generate_rafting_feedback()
"""

from datetime import datetime, timedelta
import random
import json
import uuid
import pathlib

# Predefined positive customer comments
POSITIVE_COMMENTS = [
   "The rapids provided the perfect level of challenge for our group, and our guide helped us navigate each one with expert precision and clear instructions.",
    "Watching the sunrise over the river before our morning launch was absolutely magical, setting the perfect tone for an incredible day of adventure.",
    "Our guide shared fascinating stories about local history and geology throughout the trip, making it both educational and entertaining.",
    "The teamwork required to navigate the rapids really brought our family closer together, creating memories we'll cherish for years to come.",
    "The company's attention to environmental conservation was impressive, with guides teaching us about river ecology and responsible rafting practices.",
    "From the initial safety briefing to the final group photo, every aspect of the experience was professionally managed and thoroughly enjoyable.",
    "The contrast between exciting rapids and peaceful floating sections gave us time to both get our adrenaline pumping and appreciate the natural beauty.",
    "Our guide's extensive knowledge of river navigation made us feel completely confident, even during the most challenging sections of the journey.",
    "The post-rafting barbecue was an unexpected highlight, with delicious food and great conversation about our shared adventure on the river.",
    "Every rapid had its own unique character and challenge, making the entire journey feel like an exciting series of accomplishments.",
    "The staff photographer captured amazing action shots of our raft hitting the big waves, giving us perfect memories to take home.",
    "Our guide's ability to read the river and anticipate changes made for an incredibly smooth and enjoyable rafting experience.",
    "The camaraderie that developed between different rafting groups at the lunch spot added an extra layer of fun to our adventure.",
    "Seeing the canyon walls tower above us while floating through calm sections was absolutely breathtaking and humbling.",
    "The guides coordinated perfectly with each other to ensure all rafts navigated the technical sections safely and efficiently.",
    "Learning about river reading techniques from our expert guide added an intellectual dimension to the physical adventure.",
    "The company's safety protocols were evident throughout the trip, but implemented in a way that never diminished the fun.",
    "Watching osprey dive for fish during our lunch break was a magical moment that perfectly complemented the rafting excitement.",
    "The natural hot springs stop along the river was an amazing bonus that helped soothe our muscles mid-trip.",
    "Our guide's infectious enthusiasm for rafting made even the safety briefing entertaining and engaging.",
    "The way our guide helped less confident members of the group overcome their fears was truly impressive and heartwarming.",
    "Each bend in the river revealed new spectacular views, making the whole journey feel like an expedition of discovery.",
    "The rapids provided just enough splash to keep us cool in the summer heat without being overwhelming.",
    "Our guide's weather monitoring and route adjustments ensured we had the best possible experience despite changing conditions.",
    "The cliff jumping opportunity during our lunch break added an extra element of adventure to an already exciting day.",
    "Watching the interplay of sunlight and water spray during the rapids created beautiful rainbow effects throughout our journey.",
    "The guide's extensive knowledge of local flora and fauna turned our trip into a fascinating floating nature documentary.",
    "Our teenage kids actually put down their phones and engaged with the experience, which is practically a miracle these days!",
    "The sound of rushing water and birds calling created a perfect natural soundtrack for our wilderness adventure.",
    "The guides' teamwork in handling unexpected situations showed their professionalism and extensive experience.",
    "Learning traditional river navigation techniques from our guide added a wonderful cultural dimension to the trip.",
    "The company's commitment to supporting local conservation efforts was evident in both their actions and education.",
    "Our guide's ability to gauge the group's comfort level and adjust accordingly made everyone feel included and capable.",
    "The mix of participants from different backgrounds created an interesting dynamic that enhanced the shared experience.",
    "Spotting river otters playing near the shoreline during a calm stretch was an unexpected highlight of our trip.",
    "The way our guide incorporated local indigenous history into the journey gave us a deeper appreciation for the river.",
    "Each rapid felt like a new puzzle to solve as we worked together to navigate the optimal line through the waves.",
    "The guides' constant attention to changing river conditions helped us feel secure throughout the more challenging sections.",
    "Watching the morning mist rise off the river as we started our journey created an almost mystical atmosphere.",
    "Our guide's knowledge of the best spots for photos ensured we captured amazing memories throughout the trip.",
    "The satisfaction of successfully navigating difficult rapids as a team was incredibly rewarding and confidence-building.",
    "The guides' passion for river conservation was inspiring and added an important educational element to our adventure.",
    "Even the basic paddling instruction was delivered in an engaging way that made learning the techniques fun and memorable.",
    "The riverside lunch spot offered spectacular views that made the break feel like a luxury dining experience in nature.",
    "Our guide's ability to maintain a perfect balance between safety and excitement made the trip enjoyable for everyone.",
    "The way different skill levels were accommodated within our group showed the guides' expertise in managing diverse participants.",
    "Learning about the river's seasonal changes and water patterns gave us a newfound respect for nature's power.",
    "The spontaneous water fights between rafts added an element of playful competition to our adventure.",
    "Our guide's stories about previous rafting adventures had us laughing and engaged throughout the calmer sections.",
    "The company's professional photography service captured perfect moments we were too busy enjoying to photograph ourselves.",
    "The spring runoff made for perfect water levels today, creating exciting rapids while still feeling safe and manageable for our mixed-skill group.",
    "The late summer water level was ideal for spotting river features, and our guide expertly explained how different seasons affect the rapids.",
    "Perfect weather conditions with a mix of sun and light clouds kept us comfortable throughout the trip, while the water temperature was refreshingly cool.",
    "The high water conditions added an extra thrill to the rapids, but our experienced guide navigated them masterfully while keeping everyone safe.",
    "A light morning drizzle created a mystical atmosphere on the river, followed by perfect sunny conditions for the afternoon rapids.",
    "The moderate water levels made for an ideal learning environment, allowing us to practice our paddling techniques in various conditions.",
    "The early morning fog lifting off the river created stunning photo opportunities, and the water temperature was perfect for occasional splashing.",
    "Recent rains had brought the river to an exciting level, making even the smaller rapids more engaging and entertaining.",
    "The clear skies and calm winds made for perfect rafting conditions, allowing us to fully appreciate both the rapids and scenic stretches.",
    "Water levels were optimal for seeing interesting rock formations, and our guide pointed out how the river changes throughout the seasons.",
    "The afternoon breeze kept us cool while the warm water made swimming breaks especially enjoyable during our full-day adventure.",
    "Spring flowers along the riverbank added beautiful color to our trip, and the water level was perfect for exploring side channels.",
    "The late season water levels created more technical rapids, making the navigation extra interesting and engaging for our group.",
    "A perfect mix of sunshine and scattered clouds provided ideal conditions for both photography and comfortable paddling.",
    "The river's clarity was amazing due to recent weather patterns, allowing us to see fish swimming beneath our raft in calmer sections.",
    "Mild temperatures and steady water levels made for ideal conditions to learn and practice more advanced rafting techniques.",
    "The seasonal waterfalls were spectacular due to recent rains, adding extra scenic elements to an already beautiful trip.",
    "Lower water levels revealed fascinating rock formations and river features that our guide expertly explained throughout the journey.",
    "The weather was perfectly mild, and the water level created excellent waves that were both exciting and manageable.",
    "Early summer conditions meant warmer water temperatures, making our splash-filled journey through the rapids even more enjoyable.",
    "The autumn colors along the riverbank were stunning, and the moderate water levels created perfect conditions for photography.",
    "Recent snowmelt created really fun wave trains while our guide's expertise kept us feeling secure through the bigger rapids.",
    "The weather held perfectly for our trip, with calm morning conditions building to exciting afternoon rapids as the wind picked up.",
    "Water levels were ideal for spotting wildlife along the shoreline, and our guide knew all the best viewing spots.",
    "The sunny weather made the crystal-clear water sparkle, revealing beautiful underwater features throughout our journey.",
    "Stable water levels meant we could explore some interesting side channels that aren't always accessible, adding variety to our trip.",
    "The cool morning air and warm afternoon sun created perfect conditions for our full-day rafting adventure.",
    "Higher spring flows made the rapids extra exciting, but our guide's expert navigation kept everyone comfortable and confident.",
    "The river level was perfect for practicing different paddling techniques, with our guide explaining how conditions affect strategy.",
    "Beautiful weather allowed us to fully appreciate the canyon views, while the water level created exciting but manageable rapids.",
    "The seasonal water temperature was refreshingly cool, making the sunny weather and occasional splashes perfectly balanced.",
    "Recent rainfall enhanced the waterfalls along our route without making the rapids too challenging for our mixed-ability group.",
    "The moderate water levels allowed us to really appreciate the river's natural features while still enjoying exciting rapids.",
    "Perfect visibility due to clear weather meant we could see upcoming rapids and learn about river reading techniques.",
    "The spring water levels created amazing wave trains that were incredibly fun without being too intimidating.",
    "Calm weather conditions allowed us to hear our guide's fascinating explanations about how seasonal changes affect the river.",
    "The water clarity was exceptional due to recent weather patterns, making the whole experience even more spectacular.",
    "Late summer water levels exposed interesting rock formations that our guide used to explain the river's geological history.",
    "The mild temperature and gentle breeze made for ideal rafting conditions, while the water level kept things exciting.",
    "Perfect shoulder season conditions with moderate water levels and comfortable temperatures for our full-day adventure.",
    "The recent precipitation created optimal water levels for exploring some rarely-accessible side channels.",
    "Crystal clear skies made for amazing photos, while the water level provided plenty of exciting rapid runs.",
    "The seasonal flow rates created perfect conditions for learning about river dynamics and rafting techniques.",
    "Warm air temperatures and refreshing water made for ideal swimming conditions during our lunch break.",
    "The moderate water levels allowed us to really appreciate the skill involved in navigating different types of rapids.",
    "Perfect weather for spotting wildlife, with water levels that created excellent viewing opportunities along the banks.",
    "The spring runoff added extra excitement to the rapids while our guide expertly explained seasonal water patterns.",
    "Ideal conditions for photography with clear skies and water levels that showcased the river's natural beauty.",
    "The weather was perfect for learning new paddling skills, with moderate water levels providing various challenges.",
    "Recent weather patterns created optimal conditions for exploring some of the river's more unique features.",
    "The water temperature was surprisingly comfortable thanks to recent weather patterns, making splash zones fun.",
    "Stable water levels meant we could safely navigate some more technical rapids while learning river reading skills.",
    "The sunny conditions highlighted the amazing water clarity, allowing us to spot fish during calmer stretches.",
    "Perfect rafting weather with a light breeze and water levels that created exciting but manageable rapids.",
    "The seasonal water flows made for some really fun wave trains that everyone in our group enjoyed.",
    "Ideal visibility allowed us to spot upcoming rapids and learn about different river features.",
    "The water level was perfect for practicing different paddling techniques in various rapid types.",
    "Recent rains created beautiful waterfall features without making the main rapids too challenging.",
    "Clear skies and moderate water levels made for perfect conditions to appreciate the canyon scenery.",
    "The spring flows added extra excitement while still remaining within everyone's comfort zone.",
    "Perfect weather for a full day on the river, with water levels creating consistent entertainment.",
    "The seasonal conditions created ideal opportunities for learning about river dynamics and flow patterns.",
    "Water clarity was exceptional due to recent weather, making the whole experience more engaging.",
    "The moderate water levels exposed interesting features that our guide used for navigation lessons.",
    "Perfect temperature both in and out of the water made for an incredibly comfortable adventure.",
    "The current water levels created excellent conditions for practicing more advanced rafting skills.",
    "Beautiful weather allowed us to fully appreciate both the exciting rapids and peaceful sections.",
    "The seasonal water temperature was perfect for occasional swimming breaks between rapids.",
    "Recent weather patterns created optimal conditions for exploring the river's various channels.",
    "Clear skies and steady water levels made for ideal photo opportunities throughout the trip.",
    "The water level was perfect for learning about how different rapids form and change.",
    "Mild temperatures and consistent water flows created ideal conditions for our group.",
    "The spring runoff enhanced the rapids while keeping them manageable for everyone.",
    "Perfect conditions for seeing how the river changes character throughout its course.",
    "The weather and water levels combined to create an ideal learning environment.",
    "Recent rainfall added extra features to explore without making things too challenging.",
    "The seasonal conditions were perfect for appreciating the river's natural dynamics.",
    "Clear water and steady flows made it easy to spot and navigate river features.",
    "The weather was ideal for combining exciting rapids with relaxing scenic sections.",
    "Water levels created perfect conditions for learning about river navigation.",
    "The temperature was comfortable both in and out of splash zones.",
    "Perfect visibility for spotting upcoming rapids and planning our route.",
    "The seasonal water flows made each rapid unique and interesting.",
    "Recent weather created ideal conditions for river photography.",
    "The water level was perfect for practicing different paddling strategies.",
    "Clear skies allowed us to fully appreciate the canyon's beauty.",
    "The spring conditions added excitement while maintaining safety.",
    "Perfect weather for spending a full day on the river.",
    "The current water levels made each rapid entertaining.",
    "Beautiful conditions for learning river reading techniques.",
    "The seasonal flows created consistent and enjoyable rapids.",
    "Water clarity made it easy to spot underwater features.",
    "Perfect temperature for combining rapids and swim breaks.",
    "The weather enhanced every aspect of our river adventure.",
    "Ideal conditions for practicing advanced rafting skills.",
    "The water levels made navigation both fun and educational.",
    "Recent patterns created perfect rafting conditions.",
    "Clear weather showcased the river's natural beauty.",
    "The seasonal conditions were ideal for our group.",
    "Perfect flows for learning river dynamics.",
    "The weather made every moment enjoyable.",
    "Ideal water levels throughout our journey.",
    "Beautiful conditions for river exploration.",
    "Perfect weather for rafting adventures.",
    "The seasonal flows were just right.",
    "Ideal conditions for river fun.",
    "An absolutely thrilling experience that got my adrenaline pumping! Would definitely do it again and bring all my friends next time.",
    "Our guide was fantastic throughout the entire journey, made us feel safe and confident the entire time, and really knew how to handle every situation.",
    "The rapids were intense and exactly what we were looking for! Such an incredible adrenaline rush that kept us excited the whole way down.",
    "A great weekend adventure that exceeded all my expectations, I highly recommend it to anyone looking for outdoor excitement.",
    "Loved the breathtaking scenery along the river banks, the river was beautiful and every turn revealed something new to admire.",
    "Best weekend trip I've had in years, and I've done a lot of outdoor activities! The combination of adventure and natural beauty was perfect.",
    "We got completely soaked from head to toe during the big rapids, but it was totally worth it for the incredible experience we had!",
    "The guide was so knowledgeable about the river's history and ecosystem, learned a lot about the local wildlife and geography throughout the journey.",
    "Perfect mix of excitement and relaxation throughout the day, with just the right balance of intense rapids and peaceful floating sections.",
    "Had an amazing time with family making unforgettable memories on the river, will definitely be back next year for another adventure.",
    "The guides were professional, safety-conscious, and super fun throughout the entire experience! They really made the trip special.",
    "Everything was well-organized and seamless from start to finish, with great attention to detail and customer service at every step.",
    "Loved the challenge of navigating through the rapids with our team, definitely coming back to try some more advanced sections next time.",
    "The equipment was top-notch and well-maintained, which really gave us confidence during the more challenging parts of the river journey.",
    "One of the best outdoor adventures I've ever had, combining natural beauty with excitement in ways I never expected! Truly memorable.",
    "Great experience for both beginners and experienced rafters, with excellent instruction and support from the guides throughout the journey.",
    "The lunch provided was delicious and fresh, with plenty of options for everyone, and the riverside picnic setting was absolutely perfect.",
    "The guides really knew their stuff about river safety and techniques, and made us feel comfortable even during the most challenging rapids.",
    "An unforgettable experience that created lasting memories for our whole group, can't wait to book again and try different river sections!",
    "The whole trip was a perfect balance of fun and excitement, with great moments of teamwork and individual challenges throughout the day.",
    "We saw so much amazing wildlife along the river, including eagles and deer - it was absolutely incredible and made the trip even more special!",
    "Would highly recommend this for thrill-seekers and adventure enthusiasts who want to experience nature in an exciting and unique way.",
    "Great bonding experience for our group that really brought everyone together through teamwork and shared excitement on the river.",
    "Such a peaceful yet exhilarating adventure that combined the best of nature's beauty with exciting rapid runs and calm scenic stretches.",
    "Loved the team spirit our guide encouraged throughout the trip, making everyone feel like an important part of the rafting crew.",
    "The water conditions were just perfect for rafting, with ideal levels for both exciting rapids and smooth sailing between the challenges!",
    "Felt completely safe and well-cared for the entire time, thanks to the professional guidance and excellent safety briefing before launch.",
    "A must-do experience for nature lovers who want to combine outdoor beauty with adventure and excitement on the water!",
    "We laughed so much throughout the entire trip! The guides were hilarious and kept our spirits high even during the challenging moments.",
    "Amazing views around every bend that felt like scenes from a movie, with spectacular canyon walls and pristine wilderness surrounding us.",
    "A bucket-list experience finally checked off, and it was even better than I imagined with perfect weather and outstanding guide service!",
    "The whole experience exceeded my expectations in every way, from the professional staff to the breathtaking scenery along the river.",
    "Loved the incredible rush of navigating the rapids with our team, experiencing both the excitement and satisfaction of conquering each challenge.",
    "The trip was perfectly well-paced and enjoyable for all skill levels, with great instruction that made everyone feel confident on the water.",
    "Even the calmer sections of the river were fun and engaging, with beautiful scenery and interesting stories from our guide about the area.",
    "The pre-trip instructions and safety briefing were thorough and helpful, making everyone feel prepared and confident for the adventure ahead.",
    "A great way to escape the city and enjoy nature while getting an exciting workout and making memories with friends on the river.",
    "Met some awesome people on this trip who shared our love for adventure, and the group dynamics made the experience even more enjoyable!",
    "So much fun and excitement packed into one day! Worth every penny for the memories and adventures we experienced on the river.",
    "The sunset over the river was absolutely breathtaking, creating perfect photo opportunities and memorable moments for everyone.",
    "We felt like experienced rafters thanks to the expert guidance and instruction, learning new skills while having an amazing adventure.",
    "Even my kids had an amazing time and felt safe throughout the trip! The guides were great at engaging everyone, regardless of age.",
    "Great for corporate team-building activities, combining excitement with opportunities for collaboration and trust-building on the water.",
    "The guides kept us entertained and engaged the whole time with interesting facts about the river and exciting rapid runs.",
    "Loved the pure thrill of hitting the bigger waves and rapids! The anticipation and excitement of each new challenge was incredible.",
    "Perfect mix of adventure and relaxation throughout the day, with exciting rapids balanced by peaceful moments to enjoy the scenery.",
    "I can't stop talking about this incredible trip with my friends! Every aspect was amazing, from the guides to the rapids.",
    "Saw an eagle soaring majestically above us during a calm stretch—what an incredible moment that made the trip even more special!",
    "Everything was meticulously well-planned and executed smoothly from start to finish, making the whole experience worry-free and enjoyable.",
    "Already planning my next trip with a bigger group! This experience was too good not to share with more friends and family.",
    "So much positive energy and enthusiasm from the guides throughout the entire trip, making everyone feel excited and confident on the water.",
]

# Predefined negative customer comments
NEGATIVE_COMMENTS = [
   "The water conditions were way too rough and dangerous for a beginner trip, definitely not what I expected or what was advertised to our group.",
    "Our guide seemed completely uninterested in helping us and barely engaged with the group, spending most of the time chatting with other guides instead.",
    "The equipment provided was old, worn out, and poorly maintained. Several paddles showed signs of damage that made me question their safety.",
    "The raft was overcrowded with too many people squeezed in, making it uncomfortable and difficult to paddle effectively throughout the entire trip.",
    "The safety briefing was rushed and insufficient, with not nearly enough instructions given before the trip about proper paddling techniques or emergency procedures.",
    "The rapids were far too intense for a trip marketed to beginners. Several people in our group were clearly overwhelmed and scared during the experience.",
    "Felt very unsafe at multiple points during the trip, and the guide wasn't reassuring at all when people expressed their concerns about the difficult sections.",
    "The trip was significantly overpriced for what it was, especially considering the poor quality of equipment and minimal guidance we received throughout the day.",
    "We expected a full-day adventure as advertised, but the actual time on the water felt way too short and rushed, definitely not worth the premium price.",
    "The campsite facilities were poorly maintained and clearly hadn't been cleaned in a while, with trash scattered around and broken equipment everywhere.",
    "Our guide was not engaging at all and seemed bored with the whole experience. Very disappointing compared to what was promised and what we expected.",
    "The food provided for lunch was terrible, with very limited options. Several items were stale, and there were no alternatives for dietary restrictions.",
    "The bus ride to the starting point was uncomfortably long and hot, with no air conditioning working and several unexpected delays along the way.",
    "The check-in process was inefficient and disorganized. We had to wait far too long before getting started, which cut into our actual rafting time.",
    "The restroom facilities at both the start and end points were filthy and poorly maintained, completely lacking basic supplies like toilet paper and soap.",
    "None of the safety gear provided fit properly, and when we asked for different sizes, we were told nothing else was available for our group.",
    "Most of our day was spent waiting around rather than rafting. There was too much downtime and not nearly enough actual action on the water.",
    "Communication before the trip was very poor regarding what to bring and expect. Many in our group were unprepared because of the lack of clear information.",
    "The actual experience was completely different from what was described on the website, with fewer rapids and more basic amenities than promised.",
    "Multiple staff members were consistently rude and unhelpful when we asked reasonable questions about safety and equipment concerns during our trip.",
    "The water was absolutely freezing throughout the entire trip, and no one warned us beforehand to bring appropriate clothing for the conditions.",
    "The marketing photos on the website were totally misleading, showing exciting rapids and pristine facilities that were nothing like what we experienced.",
    "Be warned - there were numerous hidden fees that weren't mentioned during booking, making the final cost way more than what was initially advertised.",
    "The wetsuits we were given were damp, smelly, and clearly hadn't been properly cleaned between uses, which made for a very uncomfortable experience.",
    "The entire experience felt extremely rushed, with no time to enjoy the scenery or take photos, as the guides were constantly hurrying us along.",
    "Despite terrible weather conditions that made the trip dangerous, no alternative dates or activities were offered, and no refunds were given.",
    "Large portions of the trip were incredibly slow and boring, with long stretches where nothing happened and the guides provided no entertainment.",
    "Without any warning or explanation, our group was split up onto different rafts, completely ruining the team-building experience we had planned.",
    "The entire booking process was needlessly complicated and frustrating, with an unclear website and unresponsive customer service team.",
    "The guides spent most of their time joking among themselves rather than ensuring our safety or enjoyment, clearly more focused on their own entertainment.",
    "The guide struggled with basic river navigation and seemed uncertain about which route to take through the rapids, making everyone nervous about their level of experience.",
    "Our guide appeared to be in training but wasn't supervised properly, missing several important safety instructions during the briefing and showing hesitation at critical moments.",
    "Very concerning that our guide couldn't properly explain basic safety procedures and kept looking to other rafts for guidance on which lines to take through the rapids.",
    "The guide's inexperience was evident when they misread simple rapids, resulting in our raft getting stuck on rocks multiple times during what should have been an easy section.",
    "Alarming how our guide seemed to be learning on the job, often asking other guides for help with basic maneuvers and safety protocols that should be second nature.",
    "Our guide admitted it was their first week leading trips solo, which became obvious when they struggled with basic paddle commands and timing through the rapids.",
    "The guide's lack of experience showed when they couldn't properly explain the safety procedures, making many in our group uncomfortable about being on the water.",
    "Really disappointed that our guide needed constant direction from other rafts, clearly showing they weren't ready to lead groups through even moderate rapids.",
    "The guide's uncertainty in handling basic river features made me question the company's training standards and wonder about their hiring practices.",
    "Our guide seemed overwhelmed by basic decision-making, frequently changing plans mid-rapid and creating unnecessary stress for everyone in the raft.",
    "Very concerning that our guide couldn't recognize simple river hazards and had to be warned by guides in other rafts about upcoming obstacles.",
    "The guide's lack of confidence in reading water conditions was apparent, leading to several close calls that could have been easily avoided with more experience.",
    "Worrying how our guide struggled with fundamental river knowledge, unable to explain basic concepts about currents and water features to the group.",
    "Our guide's inexperience became dangerous when they froze up during a moderate rapid, requiring another guide to shout instructions to our raft.",
    "Concerning that the guide couldn't maintain basic raft control in calm water, making me seriously question their ability to handle any real challenges.",
    "The guide seemed more focused on remembering their training than actually leading the raft, creating a tense atmosphere throughout the entire trip.",
    "Deeply worried by our guide's apparent lack of experience with rescue procedures, especially after they struggled to demonstrate basic safety techniques.",
    "The guide's hesitation and uncertainty during standard river maneuvers made it clear they needed more training before leading groups independently.",
    "Disappointed to learn our guide had just completed training, as their inexperience showed in every aspect of river navigation and safety management.",
    "Our guide's lack of river knowledge was evident in their inability to explain basic water patterns and currents, making the whole experience feel unsafe.",
]




# Define rafting guides (20 total, including 5 senior guides)
GUIDES = [
    "Jake", "Samantha", "Carlos", "Emily", "Tyler", "Ava", "Liam", "Sophia", 
    "Mason", "Olivia", "John (Senior Guide)", "Mary (Senior Guide)", "Henry (Senior Guide)", 
    "Laura (Senior Guide)", "James (Senior Guide)", "Grace", "Daniel", "Noah", "Emma", "Chloe"
]

# Define possible rafting trip types
TRIP_TYPES = ["Half Day", "Full Day"]

# Define the date range (Memorial Day 2024 to Labor Day 2024)
MEMORIAL_DAY_2024 = datetime(2024, 5, 24)
LABOR_DAY_2024 = datetime(2024, 9, 2)
DATE_RANGE = (LABOR_DAY_2024 - MEMORIAL_DAY_2024).days

# Ensure unique positive and negative comments
UNIQUE_POSITIVE_COMMENTS = list(set(POSITIVE_COMMENTS))
UNIQUE_NEGATIVE_COMMENTS = list(set(NEGATIVE_COMMENTS))

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

    # Generate 200 positive and 50 negative comments
    customer_remarks = [
        {
            "comment": comment,
            "character_count": len(comment),
            "guide": random.choice(GUIDES),
            "uuid": str(uuid.uuid4()),
            "date": (MEMORIAL_DAY_2024 + timedelta(days=random.randint(0, DATE_RANGE))).strftime("%Y-%m-%d"),
            "trip_type": random.choice(TRIP_TYPES),
            "timestamp": datetime.utcnow().isoformat(),
            "is_negative": False,
        }
        for comment in random.sample(UNIQUE_POSITIVE_COMMENTS, 200)
    ] + [
        {
            "comment": comment,
            "character_count": len(comment),
            "guide": random.choice(GUIDES),
            "uuid": str(uuid.uuid4()),
            "date": (MEMORIAL_DAY_2024 + timedelta(days=random.randint(0, DATE_RANGE))).strftime("%Y-%m-%d"),
            "trip_type": random.choice(TRIP_TYPES),
            "timestamp": datetime.utcnow().isoformat(),
            "is_negative": True,
        }
        for comment in random.sample(UNIQUE_NEGATIVE_COMMENTS, 50)
    ]

    # Save to a JSON file
    with open(data_file, "w") as file:
        json.dump(customer_remarks, file, indent=4)

    return data_file  # Return the path for confirmation

# Example usage:
if __name__ == "__main__":
    generated_file = generate_rafting_feedback()
    print(f"Generated rafting feedback file: {generated_file}")
