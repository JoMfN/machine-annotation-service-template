from enum import Enum


class Motivation(Enum):
    ADDING = "ods:adding"
    DELETING = "ods:deleting"
    ASSESSING = "oa:assessing"
    EDITING = "oa:editing"
    COMMENTING = "oa:commenting"
