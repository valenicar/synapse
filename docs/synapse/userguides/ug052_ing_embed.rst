Ingest - Embed Directives
=========================

The simplest example is ingesting static data which is located in the ingest file itself. This is done via an
"embed" directive. It allows us to embed nodes directly into a ingest file. We can also include secondary properties
in the files.

Here is a brief example showing an ingest containing two inet:fqdn nodes:

.. literalinclude:: ../examples/ingest_embed1.json
    :language: json

The items in the "nodes" key are a list of two-value pairs.  The first item is the form we are creating. The second
item is a list of objects that will be used to make the nodes. In this case, we simply have two ``inet:fqdn``'s listed.
If we ingest this file, if would be the equivalent of either adding nodes via Storm
(``ask [inet:fqdn=vertex.link inet:fqdn=woot.com]``) or via the Cortex formTufoByProp() API.

We can use the ingest tool (located at synapse.tools.ingest) to ingest this into a Cortex::

    ~/synapse$ python -m synapse.tools.ingest --core sqlite:///ingest_examples.db --verbose docs/synapse/examples/ingest_embed1.json
    add: inet:fqdn=com
          :host = com
          :sfx = 1
          :zone = 0
    add: inet:fqdn=woot.com
          :domain = com
          :host = woot
          :sfx = 0
          :zone = 1
    add: inet:fqdn=link
          :host = link
          :sfx = 1
          :zone = 0
    add: inet:fqdn=vertex.link
          :domain = link
          :host = vertex
          :sfx = 0
          :zone = 1
    ingest took: 0.031163692474365234 sec

Then we can open up the Cortex and see that we have made those nodes::

    ~/synapse$ python -m synapse.cortex sqlite:///ingest_examples.db
    cli> ask --props inet:fqdn=vertex.link
    inet:fqdn = vertex.link
       :domain = link
       :host = vertex
       :sfx = False
       :zone = True
       node:ndef = 42366d896b947b97e7f3b1afeb9433a3
       tufo:form = inet:fqdn
       node:created = 2018/01/05 14:52:39.035
    (1 results)

Expanding on the previous example, we can add additional forms in the embed directive - we are not limited to just a
single type of node.  Here is an example showing the addition of two ``inet:netuser`` nodes - one with a single primary
property, and one with multiple secondary properties:

.. literalinclude:: ../examples/ingest_embed2.json
    :language: json

This adds the two inet:netuser nodes to our Cortex.  We can run that with the following command to add the nodes to
our example core::

    ~/synapse$ python -m synapse.tools.ingest --core sqlite:///ingest_examples.db --verbose docs/synapse/examples/ingest_embed2.json
    add: inet:fqdn=github.com
       :domain = com
       :host = github
       :sfx = 0
       :zone = 1
    add: inet:user=bobtheuser
    add: inet:web:acct=github.com/bobtheuser
       :email = bobtheuser@gmail.com
       :seen:max = 1514764800000
       :seen:min = 1388534400000
       :site = github.com
       :user = bobtheuser
    add: inet:fqdn=gmail.com
       :domain = com
       :host = gmail
       :sfx = 0
       :zone = 1
    add: inet:email=bobtheuser@gmail.com
       :fqdn = gmail.com
       :user = bobtheuser
    add: inet:fqdn=google.com
       :domain = com
       :host = google
       :sfx = 0
       :zone = 1
    add: inet:web:acct=google.com/bobtheuser
       :site = google.com
       :user = bobtheuser
    ingest took: 0.021549463272094727 sec

Since we are using verbose mode we can see the ``inet:netuser`` nodes were created; while the existing
``inet:fqdn`` nodes were not. The default behavior for creating new nodes is to also create nodes for secondary
properties if they are also a node type.  In the example above we saw the creation of the ``inet:email``,
``inet:netuser`` and other nodes which were not explicitly defined in the ingest definition. We can confirm those
via the cmdr interface as well::

    ~/synapse$ python -m synapse.cortex sqlite:///ingest_examples.db
    cli> ask inet:web:acct
    inet:web:acct = github.com/bobtheuser
    inet:web:acct = google.com/bobtheuser
    (2 results)
    cli> ask --props inet:web:acct refs()
    inet:user     = bobtheuser
       node:ndef = 5aeefa74981e17931ae8e5ff2b947919
       tufo:form = inet:user
       node:created = 2018/01/05 14:56:12.553
    inet:email    = bobtheuser@gmail.com
       :fqdn = gmail.com
       :user = bobtheuser
       node:ndef = bc8a5ee3e8daaa19826a53b3d906af82
       tufo:form = inet:email
       node:created = 2018/01/05 14:56:12.558
    inet:fqdn     = github.com
       :domain = com
       :host = github
       :sfx = False
       :zone = True
       node:ndef = ccaecaf690eb253a1872fc8ed49b833e
       tufo:form = inet:fqdn
       node:created = 2018/01/05 14:56:12.552
    inet:web:acct = github.com/bobtheuser
       :email = bobtheuser@gmail.com
       :seen:max = 2018/01/01 00:00:00.000
       :seen:min = 2014/01/01 00:00:00.000
       :site = github.com
       :user = bobtheuser
       node:ndef = f3a453b6794ea29b6b22b0de3ef662a3
       tufo:form = inet:web:acct
       node:created = 2018/01/05 14:56:12.551
    inet:fqdn     = google.com
       :domain = com
       :host = google
       :sfx = False
       :zone = True
       node:ndef = 134b9d52f67665e86c3ab0d304030d87
       tufo:form = inet:fqdn
       node:created = 2018/01/05 14:56:12.560
    inet:web:acct = google.com/bobtheuser
       :site = google.com
       :user = bobtheuser
       node:ndef = 651e73d7ef3cc5817b0de40d85210e31
       tufo:form = inet:web:acct
       node:created = 2018/01/05 14:56:12.559
    (6 results)

Besides adding properties, we can also add `Tags`_ to the ingest files. An example below
shows adding some tags to the nodes in the embed directive. These tags can apply to either the entire set of
nodes in the embed directive (``#story.bob``) or to a single node (the one ``#src.commercial`` tag).

.. literalinclude:: ../examples/ingest_embed3.json
    :language: json

We can then apply this ingest with the following command (output omitted - it is rather long)::

    ~/synapse$ python -m synapse.tools.ingest --core sqlite:///ingest_examples.db --verbose docs/synapse/examples/ingest_embed3.json

Back in cmdr we can lift the nodes via the tags we just added::

    ~/synapse$ python -m synapse.cortex sqlite:///ingest_examples.db
    cli> ask #src.osint
    inet:web:acct = github.com/bobtheuser
       #src.osint (added 2018/01/05 15:00:00.017)
       #story.bob.accounts (added 2018/01/05 15:00:00.017)
    inet:web:acct = google.com/bobtheuser
       #src.osint (added 2018/01/05 15:00:00.017)
       #story.bob.accounts (added 2018/01/05 15:00:00.017)
    inet:fqdn     = woot.com
       #src.osint (added 2018/01/05 15:00:00.017)
       #story.bob.infrastructure (added 2018/01/05 15:00:00.017)
    (3 results)
    cli> ask #src.commercial
    inet:fqdn = vertex.link
       #src.commercial (added 2018/01/05 15:00:00.017)
       #story.bob.infrastructure (added 2018/01/05 15:00:00.017)
    (1 results)
    cli> ask #story.bob
    inet:web:acct = github.com/bobtheuser
       #src.osint (added 2018/01/05 15:00:00.017)
       #story.bob.accounts (added 2018/01/05 15:00:00.017)
    inet:web:acct = google.com/bobtheuser
       #src.osint (added 2018/01/05 15:00:00.017)
       #story.bob.accounts (added 2018/01/05 15:00:00.017)
    inet:fqdn     = vertex.link
       #src.commercial (added 2018/01/05 15:00:00.017)
       #story.bob.infrastructure (added 2018/01/05 15:00:00.017)
    inet:fqdn     = woot.com
       #src.osint (added 2018/01/05 15:00:00.017)
       #story.bob.infrastructure (added 2018/01/05 15:00:00.017)
    (4 results)
    cli> ask #story.bob.accounts
    inet:web:acct = github.com/bobtheuser
       #src.osint (added 2018/01/05 15:00:00.017)
       #story.bob.accounts (added 2018/01/05 15:00:00.017)
    inet:web:acct = google.com/bobtheuser
       #src.osint (added 2018/01/05 15:00:00.017)
       #story.bob.accounts (added 2018/01/05 15:00:00.017)
    (2 results)
    cli>

A complete example of this example embed ingest is shown below.  While the previous three ingests demonstrated
different parts of the ingest system, this is close to how the ingest file would look for longer term storage or for
doing a one-time load of data into a Cortex.

.. literalinclude:: ../examples/ingest_embed4.json
    :language: json

This can be found at the file path ``docs/synapse/examples/ingest_embed4.json`` and ingested like the
previous examples were. However, since there is nothing new to add here, there will be no new nodes created as a
result of ingesting it into ``sqlite:///ingest_examples.db``.

.. _`Tags`: ./ug008_dm_tagconcepts.html