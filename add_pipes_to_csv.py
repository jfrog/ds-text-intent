import csv

with open("temp.csv", "r", encoding='utf-8') as file:
    lines = file.readlines()
    new_file_lines = []
    i = 0
    for line in lines:
        if i == 0 or i == (len(lines)) - 1:
            line = list(line)
            line[-1] = '|'
            line.append('\n')
            print(line)
            line = ''.join(line)
            print(line)
        new_file_lines.append(line)
        i += 1

with open("temp.csv", "w") as document1:
    document1.writelines(new_file_lines)