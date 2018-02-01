class Version(object):
    def __init__(self, main_version, alpha, beta):
        self.main_version = main_version
        self.alpha = alpha
        self.beta = beta

    def __gt__(self, other):
        if self.main_version > other.main_version:
            return True
        elif self.main_version < other.main_version:
            return False
        else:
            if self.alpha > other.alpha:
                return True
            elif self.alpha < other.alpha:
                return False
            else:
                if self.beta > other.beta:
                    return True
                else:
                    return False

    def __lt__(self, other):
        return other.__gt__(self)

    def __eq__(self, other):
        return not (self.__gt__(other) or self.__lt__(other))

    def __ge__(self, other):
        return self.__gt__(other) or self.__eq__(other)

    def __le__(self, other):
        return other.__ge__(self)

    def __str__(self):
        return "{0}.{1}.{2}".format(self.main_version, self.alpha, self.beta)

    __repr__ = __str__

    @staticmethod
    def strpversion(version_str):
        parts = version_str.split(".")
        return Version(int(parts[0]), int(parts[1]), int(parts[2]))