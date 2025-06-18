from Commands.CommandCreator import CommandCreator
from Utils.Encoder import Encoder
from logging import getLogger, DEBUG, basicConfig
from queue import Queue

log = getLogger("logger")
basicConfig(level=DEBUG)

CMD_CREATOR = CommandCreator()
ENCODER = Encoder()

target_groups_ids: set = {"public192867633", 
                           "lovelycosplay", 
                           "miptdndleague", 
                           "stfpmi", 
                           "anifox", 
                           "fpmi_edu", 
                           "reddit", 
                           "twinsrpg", 
                           "marsbgame", 
                           "best_rpg_games", 
                           "starcraft_memes", 
                           "dit2225", 
                           "lore_dnd", 
                           "fpmi_students", 
                           "kleientertainment", 
                           "anime", 
                           "knigiprint", 
                           "cpp_lib", 
                           "resf"}

DEFAULT_TOPIC_PREFIX: str = ""
KAFKA_ADMIN_COMMANDS_TOPIC_NAME: str = "kafka_admin_commands"
FETCHER_ADMIN_COMMANDS_TOPIC_NAME: str = "fetcher_admin_commands"

VK_FETCHERS_COMMANDS_TOPIC_NAME: str = "vk_fetcher_commands"
VK_PULLING_TASKS_TOPIC_NAME: str = "vk_pulling_tasks"
VK_PULLED_POSTS_TOPIC_NAME: str = "vk_posts"

pulling_tasks_queue: Queue = Queue()