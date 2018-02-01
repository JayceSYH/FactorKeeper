def main():
    import os, sys
    # add parent directory to python working directory
    dir_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if dir_path not in sys.path:
        sys.path.append(dir_path)

    # SkyEcon module import
    from Core.WorkerNode.WorkerNode import Worker
    from Core.WorkerNode.CGI.CGIRunner import CGIRunner

    # pid control
    import os
    with open("../Tmp/NameNode.pid", "w") as f:
        f.write(str(os.getpid()))

    worker_node = Worker()
    cgirunner = CGIRunner(worker_node)
    cgirunner.run()
