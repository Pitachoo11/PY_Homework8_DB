# Главное меню справочника организации

from functions import export_txt, export_csv, input_DB, search_name

command = 0

while command != 5:
    print ('*************************************')
    print (' СПРАВОЧНИК ОРГАНИЗАЦИИ')
    print (' Нажмите: ')
    print ('1 - Для загрузки начальных данных в базу')
    print ('2 - Для поиска контактной информации по фамилии')
    print ('3 - Для конвертации ВСЕЙ базы в формат csv')
    print ('4 - Для конвертации ВСЕЙ базы в формат txt')
    print ('5 - Для завершения программы')
    print ('*************************************')
    try:
        command =int(input())
        if command == 1:
            input_DB()
        elif command == 2:
            print('Введите ФАМИЛИЮ для поиска:')
            name = input()
            search_name(name)
            break 
        elif command == 3:
            export_csv()
        elif command == 4:
            export_txt()   
        elif command == 5:
            break
    except:
        print ('Повторите ввод')

    
