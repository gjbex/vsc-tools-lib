'''module for dealing with Moab's checknode output'''

from xml.dom import minidom

class ChecknodeParser(object):
    '''Parser class for Moab checknode ouptut'''

    def parse(self, checknode_xml):
        '''parse checknode XML output, and return the list of features'''
        dom = minidom.parseString(checknode_xml)
        node = dom.getElementsByTagName('node')[0]
        feature_str = node.getAttribute('FEATURES')
        return feature_str.split(',')

    def parse_file(self, node_file):
        '''parse a file that contains checknode output'''
        node_output = ''.join(node_file.readlines())
        return self.parse(node_output)
