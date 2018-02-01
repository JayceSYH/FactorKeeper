if __name__ == "__main__":
    import sys, os
    FACTOR_KEEPER_BASE = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    sys.path.append(FACTOR_KEEPER_BASE)

    import Core.NameNode.NameNodeServer as NameNodeServer
    NameNodeServer.main()
