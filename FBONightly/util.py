def slurp(fname, decode="utf-8"):
    with open(fname, 'rb') as FH:
        return FH.read().decode(decode)

