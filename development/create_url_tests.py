import csv

with open('input.csv', 'w+') as f:
    write = csv.writer(f, quoting=csv.QUOTE_ALL)
    write.writerow(["body", "improved_emails", "quote_label", "date", "fold"])
    write.writerow(["ciao, vorrei una traduzione/n","ciao, vorrei una traduzione", "1" , "2022", "."])
