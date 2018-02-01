def main():
    import os, sys
    # add parent directory to python working directory
    dir_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if dir_path not in sys.path:
        sys.path.append(dir_path)

    # SkyEcon module import
    from Core.NameNode.NameNode import Master
    from Core.NameNode.CGI.CGIRunner import CGIRunner

    # pid control
    import os
    with open("../Tmp/NameNode.pid", "w") as f:
        f.write(str(os.getpid()))

    name_node = Master()
    cgirunner = CGIRunner(name_node)
    cgirunner.run()
