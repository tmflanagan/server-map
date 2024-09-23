#!/opt/server-map/bin/python3

import argparse
import datetime
from datetime import timedelta
from dateutil import relativedelta
import functools
import json
import matplotlib.pyplot as plt
import networkx as nx
import os
import pyhocon
import re
import warnings
import yaml

p = argparse.ArgumentParser(description='Takes an environment config file and server config files and outputs the relevant data stream information.')
p.add_argument('-e','--environment',type=str,required=True,help='location of config YAML file')
p.add_argument('-s','--servers',nargs='+',type=str,required=True,help='space-separated list of server config HOCON files')
p.add_argument('-o','--output',nargs=2,type=str,required=False,default='pod_architecture.json graph.png'.split(),help='path to output architecture JSON and map PNG')
args = p.parse_args()

def main():
    environment_keys = ['API_PKG', 'STREAM_PKG', 'ALT_STREAM_PKG'] # keys of items in the environment config to extract
    scheme = NamingScheme(formats=['_PKG','_SVC_PKG'], mutations=['_CONF','_UNPACKER_CONF']) # current naming scheme within the environment config files

    # Step 1: Reads data out of environment config YAML, prints everything in 'data' to the command line, and stores every match of the environment keys with the naming scheme to the list "confs"
    with open(args.environment, 'rb') as f:
        data = next(yaml.safe_load_all(f))['data']
        print('Complete scheme.formats match dump for ' + args.environment + ': [' + ', '.join(scheme.dumpMatches(list(data))) + ']')
        confs = scheme.matchKeys(data, environment_keys)

    # Step 2: Iterates through every config referenced in the environment keys and adds each unique TreeElement (alongside its inputs and outputs) to a dict of "nodes", merging repeats along the way
    nodes = {}
    for config in confs:
        buildNum = re.search(r'v\d+$', config[list(config)[0]]).group(1)
        for server in list(config)[1:]:
            for filePath in args.servers:
                if os.path.split(config[server])[-1] in filePath: # Requires identical filenames to those in dev-config
                    currentServer = serverConstructor(server, filePath, buildNum)
            if isinstance(currentServer, TreeElement): # Ensures every node has a valid constructor
                nodes.update({currentServer.nickname : currentServer})
                for n in (currentServer.getInputs() + currentServer.getOutputs()):
                    if n.nickname in nodes:
                        n = nodes[n.nickname].merge(n)
                    else:
                        nodes.update({n.nickname : n})

    # Step 3: Encodes architecture as JSON and writes to file
    with open(args.output[0], 'w') as f:
        dump = json.dumps(nodes, sort_keys=True, cls=CustomJSONEncoder, indent=2)
        print(dump)
        f.write(dump)

    # Step 4: Iterates through the nodes dict to find each unique edge, adds it to a graph, and draws it to a networkx plot
    G = nx.DiGraph()
    for node in nodes.values(): # graphs each edge once based on the union of every node's inputs
        if hasattr(node, 'colorHandler'): # colored edge handler
            for i in node.getInputs():
                color = node.colorHandler(i)
                G.add_edge(i.nickname, node.nickname, edge_color=color)
        else: # defaults to black edges
            for i in node.getInputs():
                G.add_edge(i.nickname, node.nickname, edge_color='black')
    ec = [G[u][v]['edge_color'] for u,v in G.edges()]
    plt.figure(figsize=(15,15))
    G = nx.relabel_nodes(G, {'': '[empty]'}) # graphviz can't handle empty strings
    nx.draw(G, pos=nx.drawing.nx_agraph.graphviz_layout(G, prog='dot', args="-Grankdir=LR"), edge_color=ec, with_labels=True, width=3, bbox=dict(facecolor='firebrick', boxstyle='round,pad=0.2'))
    #plt.show()
    plt.savefig(args.output[1])

# Handles identification of server type based on the contents of the config file and returns the corresponding treeElement
def serverConstructor(name, filename, buildNum):
    name = re.sub(r'_CONF', '', name)
    with open(filename, 'r') as f:
        data = pyhocon.ConfigFactory.parse_string(f.read())
        if 'logstreams' in data:
            return ApiElement(name, data, buildNum)
        elif 'packed' in data:
            return StreamElement(name, data, buildNum)
        else:
            warnings.warn('File "' + filename + '" of unknown type')

# Overrides the default python JSON encoder to serialze timedelta information stored in config files
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, timedelta) or isinstance(obj, relativedelta.relativedelta):
            return str(obj)
        if isinstance(obj, TreeElement):
            return print(obj)
        # Add more custom serialization logic here for other types if needed
        return super().default(obj)

# Essentially a paired list of regular expressions, where "formats" are replaced with their corresponding "mutation" to match multiple config files with related environment information
class NamingScheme():
    def __init__(self, **regexes):
        self.formats = regexes['formats']
        self.mutations = dict(zip(self.formats,regexes['mutations']))

    # searches through some list keys for any values matching a format, returns a list of any extant formats and mutated matches
    def dumpMatches(self, keys):
        matches = []
        for key in keys:
            for f in self.formats:
                if re.search(f, key):
                    matches.append(key)
                    subSearch = re.sub(f, self.mutations[f], key)
                    for m in keys:
                        if re.search(subSearch, m):
                            matches.append(m)
        return matches

    # searches through some dict "source" given some subset of its keys "keys" and returns a dict of all matches of it and its mutations
    def matchKeys(self, source, keys):
        matches = []
        for i in range(len(keys)):
            matches.append({keys[i] : source[keys[i]]})
            for f in self.formats:
                mutKey = re.sub(f, self.mutations[f], keys[i])
                for s in list(source):
                    if re.search(mutKey, s):
                        matches[i].update({s : source[s]})
        return matches

# A generic class for a node in the overall graph
class TreeElement:
    def __init__(self, name, source, isVisible=True):
        self.name = name
        self.nickname = name
        self.source = source
        self.isVisible = isVisible
        self.inputs = []
        self.outputs = []

    # Checks for equal type, then each non-list element, then the names of inputs and outputs (if the inputs and outputs themselves were compared, there's an infinite loop). Does not account for any subclass attributes
    def __eq__(self, other):
        if isinstance(other, type(self)):
            if self.name == other.name and self.source == other.source and self.isVisible == other.isVisible and len(self.inputs)==len(other.inputs) and len(self.outputs)==len(other.outputs):
                sInputs = []
                oInputs = []
                for i in range(len(self.inputs)):
                    sInputs.append(self.inputs[i].name)
                    oInputs.append(other.inputs[i].name)
                sOutputs = []
                oOutputs = []
                for i in range(len(self.outputs)):
                    sOutputs.append(self.outputs[i].name)
                    oOutputs.append(other.outputs[i].name)
                return (sInputs.sort()==oInputs.sort() and sOutputs.sort()==oOutputs.sort())
        return False

    def __str__(self):
        inputNames = []
        for i in self.inputs:
            inputNames.append(i.name)
        outputNames = []
        for i in self.outputs:
            outputNames.append(i.name)
        return 'Name: ' + self.name + '\nNickname: ' + self.nickname + '\nInputs: [' + ', '.join(inputNames) + ']\nOutputs: [' + ', '.join(outputNames) + ']\n'

    # Any subclass with a corresponding config document should override this with its own handler
    def populateIO(self):
        pass

    # Returns a list of only visible inputs. If there are invisible input(s), it returns their visible inputs.
    def getInputs(self):
        visibleInputs = []
        for i in self.inputs:
            if i.isVisible:
                visibleInputs.append(i)
            else:
                for sub in i.getInputs():
                    visibleInputs.append(sub)
        return visibleInputs

    # Returns a list of only visible outputs. If there are invisible output(s), it returns their visible outputs.
    def getOutputs(self):
        visibleOutputs = []
        for i in self.outputs:
            if i.isVisible:
                visibleOutputs.append(i)
            else:
                for sub in i.getOutputs():
                    visibleOutputs.append(sub)
        return visibleOutputs

    # Adds any inputs/outputs of other not present in self to the input and output arrays, and then returns itself
    def merge(self, other):
        for element in other.inputs:
            if element not in self.inputs:
                self.inputs.append(element)
        for element in other.outputs:
            if element not in self.outputs:
                self.outputs.append(element)
        return self

# A TreeElement specified for the example API Server config document
class ApiElement(TreeElement):
    keys = ['logstreams','persistence']

    def __init__(self, name, source, buildNum):
        super().__init__(name, source)
        self.buildNum = buildNum
        self.populateIO()

    def __str__(self):
        lines = super().__str__().split('\n')
        lines[2:2] = ['Build #: ' + str(self.buildNum)]
        return '\n'.join(lines)

    def populateIO(self):
        if self.keys[0] in self.source:
            for stream in self.source[self.keys[0]]:
                foundOutput = KafkaTopic(stream['create']['log.topic'], {})
                self.outputs.append(foundOutput)
                self.outputs[-1].inputs.append(self)
        if self.keys[1] in self.source:
            if 'collection' in self.source[self.keys[1]]:
                for recType in list(self.source[self.keys[1]]['collection']):
                    foundInput = MongoCollection(recType, {})
                    self.inputs.append(foundInput)
                    self.inputs[-1].outputs.append(self)
            if 'dedicated_partitions' in self.source[self.keys[1]]:
                    self.partitions = self.source[self.keys[1]]['dedicated_partitions']

# A TreeElement specified for the example Stream server config documents
class StreamElement(TreeElement):
    keys = ['apiEndpoint','packed','unpacked','agents_unpacked','updates']
    colorMap = dict(zip(keys, ['black', 'khaki', 'yellow', 'cadetblue', 'darkcyan']))

    def __init__(self, name, source, buildNum=-1):
        super().__init__(name, source)
        self.buildNum = buildNum
        self.groups = []
        self.populateIO()

    def __str__(self):
        lines = super().__str__().split('\n')
        lines[2:2] = ['Build #: ' + str(self.buildNum), 'Groups: [' + ', '.join(self.groups) + ']']
        return '\n'.join(lines)

    def populateIO(self):
        for key in self.keys:
            if key in self.source:
                if key == 'apiEndpoint':
                    foundOutput = TreeElement(self.source[key],{})
                    self.outputs.append(foundOutput)
                    self.outputs[-1].inputs.append(self)
                else:
                    self.groups.append(key)
                    foundInput = KafkaTopic(self.source[key]['topic'], {})
                    self.inputs.append(foundInput)
                    self.inputs[-1].streamInput(self, key)

    def colorHandler(self, other):
        for i in range(len(self.inputs)):
            if self.inputs[i] == other:
                return self.colorMap[self.groups[i]]
        return 'black'

# A TreeElement which removes a commmon prefix from MongoDB collections referenced in the example server config documents
class MongoCollection(TreeElement):
    def __init__(self, name, source):
        super().__init__(name, source)
        self.nickname = re.sub(r'prefix\.|[\"]', '', name)

# A TreeElement which removes a common prefix from Kafka topics referenced in the example server config documents
class KafkaTopic(TreeElement):
    def __init__(self, name, source):
        super().__init__(name, source)
        self.nickname = re.sub(r'alt-prefix-', '', name)
        self.outputGroups = {}

    def streamInput(self, other, key):
        self.outputs.append(other)
        self.outputGroups.update({other.nickname : key})

if __name__ == "__main__":
    main()
