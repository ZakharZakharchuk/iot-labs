class InfiniteRepetitiveRange:
    @staticmethod
    def infinite_repetitive_range(n):
        while True:
            for i in range(n):
                yield i
