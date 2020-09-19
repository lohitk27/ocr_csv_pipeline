# -*- coding: utf-8 -*-
"""

"""

class Node:
    def __init__(self, label=None, data=None):
        self.label = label
        self.data = data
        self.children = dict()
    
    def addChild(self, key, data=None):
        if not isinstance(key, Node):
            self.children[key] = Node(key, data)
        else:
            self.children[key.label] = key
    
    def __getitem__(self, key):
        return self.children[key]

class Trie:
    def __init__(self):
        self.head = Node()
    
    def __getitem__(self, key):
        return self.head.children[key]
    
    def add(self, phrase):
        current_node = self.head
        phrase_finished = True
        phrasewords_list =[]
        for word in phrase.split( ):
            phrasewords_list.append(word)
            
        for i in phrasewords_list:
            if i in current_node.children:
                current_node = current_node.children[i]
            else:
                phrase_finished = False
                break
        
        # For ever new letter, create a new child node
        if not phrase_finished:
            i = 0
            while i < len(phrasewords_list):
                #print 'i in while loop'
                #print phrasewords_list[i]
                current_node.addChild(phrasewords_list[i])
                #print current_node
                current_node = current_node.children[phrasewords_list[i]]
                #print current_node
                i += 1
        
        # Let's store the full word at the end node so we don't need to
        # travel back up the tree to reconstruct the word
        current_node.data = phrase
    
    def has_word(self, phrase):
        if phrase == '':
            return False
        if phrase == None:
            raise ValueError('Trie.has_word requires a not-Null string')
        
        # Start at the top
        current_node = self.head
        text_list =[]
        matching_list =[]
        Attribute_list =[]
        for word in phrase.split( ):
            text_list.append(word)
        for i in text_list:
            if i in current_node.children:
                current_node = current_node.children[i]
                matching_list.append(i)
                if not current_node.children:
                    Attribute_list.append(' '.join(matching_list))
                    
            else:
                current_node = self.head
                matching_list =[]
                continue
        return Attribute_list
        

if __name__ == '__main__':
    """ Example use """
    trie = Trie()
    ######## list of strings/ phrases to be searching
    list = ['jass','assse']
    for i in list:
        trie.add(i)
    print("'Phrases find' in trie: ", trie.has_word('abscff, agdajff jasasss  are good friends'))
    #print "'goodbye help' in trie: ", trie.has_word('goodbye help')