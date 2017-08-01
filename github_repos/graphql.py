class GraphQLNode:
    # Named something that hopefully won't conflict with any graphql names, about 91 bits of entropy here
    # Wish I had gensym here...
    def __init__(self, tUqFUwfpLIkrUAjy, **kwargs):
        self.name = tUqFUwfpLIkrUAjy

        args = []
        for k, v in kwargs.items():
            if v is not None:
                args.append((k, v))
        self.args = tuple(args)

        self.children = ()

    def __call__(self, *nodes):
        new_children = []
        for node in nodes:
            if node:
                if isinstance(node, GraphQLNode):
                    new_children.append(node)
                elif isinstance(node, str):
                    new_children.append(GraphQLNode(node))
                else:
                    e = TypeError('Each child to a GraphQLNode must be a GraphQLNode or string')
                    raise e

        self.children = tuple(list(self.children) + new_children)
        return self

    # TODO pretty-print
    def format(self):
        if self.args:
            argstr = '({}: {!r}'.format(*self.args[0])
            for arg in self.args[1:]:
                argstr += ', {}: {!r}'.format(*arg)
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

        return '{name} {args}{children}'.format(name=self.name, args=argstr, children=childrenstr)

    def export(self):
        exported_children = []
        for child in self.children:
            if isinstance(child, GraphQLNode):
                exported_children.append(child.export())
            else:
                exported_children.append(child)

        return (self.name, self.args, tuple(exported_children))
