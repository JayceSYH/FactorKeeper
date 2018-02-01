if __name__ == "__main__":
    import psutil

    for pid in psutil.pids():
        try:
            p = psutil.Process(pid)
            cmds = p.cmdline()
            if len(cmds) > 1 and "start_workernode.py" in cmds[1]:
                p.kill()
        except Exception as e:
            print(e)