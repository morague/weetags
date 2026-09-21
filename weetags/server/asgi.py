from weetags.server.main import Weetags

weetags = Weetags.create_app("./server.yml")
app = weetags()
