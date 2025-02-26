## Author: Ashantha Rosary
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

class EmojiHandler:
    def __init__(self, emoji_dict):
        """
        Initialize with the emoji dictionary.
        """
        self.emoji_dict = emoji_dict

    def replace_emojis(self, text):
        """
        Replace emojis in the text using the provided dictionary and remove non-mapped emojis.
        """
        if text:
            # Replace emojis using the dictionary
            for emoji, replacement in self.emoji_dict.items():
                text = text.replace(emoji, replacement)
            # Remove any remaining emojis that were not in the dictionary
            text = re.sub(r'[^\x00-\x7F]+', '', text)  # This regex removes non-ASCII characters (i.e., emojis)
        return text

    def get_replace_emojis_udf(self):
        """
        Return a UDF to be applied in a PySpark DataFrame for emoji mapping and removal.
        """
        return udf(lambda text: self.replace_emojis(text), StringType())
