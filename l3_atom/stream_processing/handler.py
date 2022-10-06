from l3_atom.stream_processing.standardisers import standardisers

def initialise_agents(app):
    for standardiser in standardisers:
        agent = standardiser()
        standardiser.raw_topic = app.topic(agent.raw_topic)
        print(f"Initialising {agent.id} standardiser")
        app.agent(standardiser.raw_topic, name=f"{agent.id}_agent")(agent.process)
        for k, v in agent.normalised_topics.items():
            agent.normalised_topics[k] = app.topic(v)