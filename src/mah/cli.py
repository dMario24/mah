import argparse
from mah.db.utils import count, top

def hello_msg():
    return "hello"

def cmd():
    msg = hello_msg()
    #print(msg)

    parser = argparse.ArgumentParser(
                    prog='ProgramName',
                    description='What the program does',
                    epilog='Text at the bottom of help')

    parser.add_argument('-s', '--scount') 
    parser.add_argument('-t', '--top', type=int)
    parser.add_argument('-d', '--dt' )
    parser.add_argument('-p', '--pretty', action='store_true')

    args = parser.parse_args()
    #print(args.scount, args.top, args.dt)

    if args.scount: 
        # print(f"-s => {args.scount}")
        # TODO 명령어 카운트
        r = count(args.scount)
        print (f"{args.scount} 사용 횟수는 {r}회 입니다.")
    elif args.top:
        # print(f"-t => {args.top}")
        if args.dt:
            # print(f"-d => {args.dt}")
            # TODO 특정 날짜의 명령어 TOP N
            r = top(args.top, args.dt, args.pretty)
            #r = top(int("10"), "2024-07-17")
            print(r)
        else:
            parser.error("-t 옵션은 -d 옵션과 함께 사용하시오!")
    else:
        parser.print_help()

