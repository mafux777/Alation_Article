registry={}

class Folder():
    def __init__(self, name, parent=None):
        self.name = name
        self.parent = parent
        r = f'{self}'
        if r not in registry:
            registry[r] = self
    def __repr__(self):
        if self.parent:
            return f'{self.parent}/{self.name}'
        else:
            return '' # root does not have a name

    def get_or_create_from_path(self, path):
        path_items = path.split('/')
        n = len(path_items)
        if n == 1:
            return Folder(path_items[0], self)
        # reduce phase... make first item parent
        p = self.get_or_create_from_path(path_items[0])
        # call recursively, but with one less item on the path
        q = p.get_or_create_from_path('/'.join(path_items[1:]))
        return q




root=Folder('/')

print(root.get_or_create_from_path('a/b/c'))

d=(root.get_or_create_from_path('d'))
print(d.get_or_create_from_path('e/f/g'))
print(root.get_or_create_from_path('fd/d/f/g'))
print(root.get_or_create_from_path('d/d/f/g'))
print()

