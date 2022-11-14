import pandas as pd

df = pd.read_csv("/home/alation/users.csv")

for n, u in df.iterrows():
    if not isinstance(u['email'], float):
        my_user = User.objects.filter(email__iexact=u['email'])
        if my_user.count() == 0:
            print(f"User {u['email']} not found at all.")
        elif my_user.count() == 1:
            x = my_user.last()
            if not isinstance(u['Username'], float) and '@' in u['Username']:
                x.username = u['Username']
                x.save()
                print(f"Fixed: {u['Username']}")
            else:
                x.profile.suspend()
                print(f"Suspended: {u['Username']}")
        else:
            print(f"Multiple users found: {my_user.values('username')}")
    else:
        print(f"Nothing to do for {u}")
