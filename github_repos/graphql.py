import json

class Node:
    # Named something that hopefully won't conflict with any graphql names, about 91 bits of entropy here
    # Wish I had gensym here...
    def __init__(self, *tUqFUwfpLIkrUAjy, **kwargs):
        args = tUqFUwfpLIkrUAjy
        if len(args) not in (1, 2):
            e = ValueError('__init__ takes either a GraphQL node identifier, or a json key and node identifier. ' +
                           'You supplied {} positional arguments'.format(len(args)))
            raise e

        if len(args) == 2:
            self.json_name = args[0]
            self.gql_name = args[1]
        else:
            self.json_name = None
            self.gql_name = args[0]

        gql_args = []
        for k, v in kwargs.items():
            if v is not None:
                gql_args.append((k, v))
        self.args = tuple(gql_args)

        self.children = ()

    def __call__(self, *nodes):
        new_children = []
        for node in nodes:
            if node:
                if isinstance(node, Node):
                    new_children.append(node)
                elif isinstance(node, str):
                    new_children.append(Node(node))
                else:
                    e = TypeError('Each child to a Node must be a Node or string')
                    raise e

        self.children = tuple(list(self.children) + new_children)
        return self

    # TODO pretty-print
    def format(self):
        if self.json_name:
            namestr = self.json_name + ': '
        else:
            namestr = ''

        if self.args:
            argstr = '({}: {}'.format(self.args[0][0], json.dumps(self.args[0][1]))
            for arg in self.args[1:]:
                argstr += ', {}: {}'.format(arg[0], json.dumps(arg[1]))
            argstr += ')'
        else:
            argstr = ''

        if self.children:
            childrenstr = '{ ' + self.children[0].format()
            for child in self.children[1:]:
                childrenstr += ', ' + child.format()
            childrenstr += ' }'
        else:
            childrenstr = ''

        return '{jsonname}{name} {args}{children}'.format(
            jsonname=namestr, name=self.gql_name, args=argstr, children=childrenstr)

    def export(self):
        exported_children = []
        for child in self.children:
            if isinstance(child, Node):
                exported_children.append(child.export())
            else:
                exported_children.append(child)

        return (self.gql_name, self.args, tuple(exported_children))

