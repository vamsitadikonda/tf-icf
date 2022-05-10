actual={}
predicted={}

def read_dict(path,d):
    with open(path) as fp:
        lines = fp.readlines()
        for line in lines:
            k,v = line.strip('\n').split('\t')
            d[k] = v

read_dict("/Users/vamsitadikonda/Documents/academics/ParallelSys/HW/HW4/p1/curr/output0/WordCount/part-r-00000",predicted)
read_dict("/Users/vamsitadikonda/Documents/academics/ParallelSys/HW/HW4/p1/output0/WordCount/part-r-00000",actual)

print(predicted.keys() - actual.keys())
print("\n\n\n\n\n")
print(actual.keys() - predicted.keys())