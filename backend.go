package dukedb

type JoinAssigner func(objs, joinedModels []interface{}, joinQ *RelationQuery)
