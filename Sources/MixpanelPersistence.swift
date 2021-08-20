//
//  MixpanelPersistence.swift
//  Mixpanel
//
//  Created by ZIHE JIA on 7/9/21.
//  Copyright © 2021 Mixpanel. All rights reserved.
//

import Foundation

enum PersistenceType: String, CaseIterable {
    case events
    case people
    case groups
}

struct PersistenceConstant {
    static let unIdentifiedFlag = true
}


class MixpanelPersistence {
    
    let apiToken: String
    let mpdb: MPDB
    
    init(token: String) {
        apiToken = token
        mpdb = MPDB.init(token: apiToken)
    }
    
    
    func saveEntity(_ entity: InternalProperties, type: PersistenceType, flag: Bool = false) {
        if let data = JSONHandler.serializeJSONObject(entity) {
            mpdb.insertRow(type, data: data, flag: flag)
        }
    }
    
    func saveEntities(_ entities: Queue, type: PersistenceType, flag: Bool = false) {
        for entity in entities {
            if let data = JSONHandler.serializeJSONObject(entity) {
                mpdb.insertRow(type, data: data, flag: flag)
            }
        }
    }
    
    func loadEntitiesInBatch(type: PersistenceType, batchSize: Int = 50, flag: Bool = false) -> [InternalProperties] {
        let dataMap = mpdb.readRows(type, numRows: batchSize, flag: flag)
        var jsonArray : [InternalProperties] = []
        for (key, value) in dataMap {
            if let jsonObject = JSONHandler.deserializeData(value) as? InternalProperties {
                var entity = jsonObject
                entity["id"] = key
                jsonArray.append(entity)
            }
        }
        return jsonArray
    }
    
    func removeEntitiesInBatch(type: PersistenceType, ids: [Int32]) {
        mpdb.deleteRows(type, ids: ids)
    }
    
    func identifyPeople(token: String) {
        mpdb.updateRowsFlag(.people, newFlag: !PersistenceConstant.unIdentifiedFlag)
    }
    
    func resetEntities() {
        for pType in PersistenceType.allCases {
            mpdb.deleteRows(pType)
        }
    }
    
    func saveOptOutStatusFlag(value: Bool) {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return
        }
        let prefix = "mixpanel-\(apiToken)-"
        defaults.setValue(value, forKey: prefix + "OptOutStatus")
        defaults.synchronize()
    }
    
    func loadOptOutStatusFlag() -> Bool {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return false
        }
        let prefix = "mixpanel-\(apiToken)-"
        return defaults.bool(forKey: prefix + "OptOutStatus")
    }
    
    
    func saveAutomacticEventsEnabledFlag(value: Bool, fromDecide: Bool) {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return
        }
        let prefix = "mixpanel-\(apiToken)-"
        if fromDecide {
            defaults.setValue(value, forKey: prefix + "AutomaticEventEnabledFromDecide")
        } else {
            defaults.setValue(value, forKey: prefix + "AutomaticEventEnabled")
        }
        defaults.synchronize()
    }
    
    func loadAutomacticEventsEnabledFlag() -> Bool {
        #if TV_AUTO_EVENTS
        return true
        #else
        let prefix = "mixpanel-\(apiToken)-"
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return false
        }
        return defaults.bool(forKey: prefix + "AutomaticEventEnabled") || defaults.bool(forKey: prefix + "AutomaticEventEnabledFromDecide")
        #endif
    }
    
    func saveTimedEvents(timedEvents: InternalProperties) {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return
        }
        let prefix = "mixpanel-\(apiToken)-"
        let timedEventsData = NSKeyedArchiver.archivedData(withRootObject: timedEvents)
        defaults.set(timedEventsData, forKey: prefix + "timedEvents")
        defaults.synchronize()
    }
    
    func loadTimedEvents() -> InternalProperties {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return InternalProperties()
        }
        let prefix = "mixpanel-\(apiToken)-"
        guard let timedEventsData  = defaults.data(forKey: prefix + "timedEvents") else {
            return InternalProperties()
        }
        return NSKeyedUnarchiver.unarchiveObject(with: timedEventsData) as? InternalProperties ?? InternalProperties()
    }
    
    func saveSuperProperties(superProperties: InternalProperties) {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return
        }
        let prefix = "mixpanel-\(apiToken)-"
        let superPropertiesData = NSKeyedArchiver.archivedData(withRootObject: superProperties)
        defaults.set(superPropertiesData, forKey: prefix + "superProperties")
        defaults.synchronize()
    }
    
    func loadSuperProperties() -> InternalProperties {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return InternalProperties()
        }
        let prefix = "mixpanel-\(apiToken)-"
        guard let superPropertiesData  = defaults.data(forKey: prefix + "superProperties") else {
            return InternalProperties()
        }
        return NSKeyedUnarchiver.unarchiveObject(with: superPropertiesData) as? InternalProperties ?? InternalProperties()
    }
    
    func saveIdentity(distinctID: String, peopleDistinctID: String?, anonymousID: String?, userID: String?, alias: String?, hadPersistedDistinctId: Bool?) {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return
        }
        let prefix = "mixpanel-\(apiToken)-"
        defaults.set(distinctID, forKey: prefix + "MPDistinctID")
        defaults.set(peopleDistinctID, forKey: prefix + "MPPeopleDistinctID")
        defaults.set(anonymousID, forKey: prefix + "MPAnonymousId")
        defaults.set(userID, forKey: prefix + "MPUserId")
        defaults.set(alias, forKey: prefix + "MPAlias")
        defaults.set(hadPersistedDistinctId, forKey: prefix + "MPHadPersistedDistinctId")
        defaults.synchronize()
    }
    
    func loadIdentity() -> (String, String?, String?, String?, String?, Bool?) {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return ("", nil, nil, nil, nil, nil)
        }
        let prefix = "mixpanel-\(apiToken)-"
        return (defaults.string(forKey: prefix + "MPDistinctID") ?? "",
                defaults.string(forKey: prefix + "MPPeopleDistinctID"),
                defaults.string(forKey: prefix + "MPAnonymousId"),
                defaults.string(forKey: prefix + "MPUserId"),
                defaults.string(forKey: prefix + "MPAlias"),
                defaults.bool(forKey: prefix + "MPHadPersistedDistinctId"))
    }
    
    func deleteMPUserDefaultsData() {
        guard let defaults = UserDefaults(suiteName: "Mixpanel") else {
            return
        }
        let prefix = "mixpanel-\(apiToken)-"
        defaults.removeObject(forKey: prefix + "MPDistinctID")
        defaults.removeObject(forKey: prefix + "MPPeopleDistinctID")
        defaults.removeObject(forKey: prefix + "MPAnonymousId")
        defaults.removeObject(forKey: prefix + "MPUserId")
        defaults.removeObject(forKey: prefix + "MPAlias")
        defaults.removeObject(forKey: prefix + "MPHadPersistedDistinctId")
        defaults.removeObject(forKey: prefix + "AutomaticEventEnabled")
        defaults.removeObject(forKey: prefix + "AutomaticEventEnabledFromDecide")
        defaults.removeObject(forKey: prefix + "OptOutStatus")
        defaults.removeObject(forKey: prefix + "timedEvents")
        defaults.removeObject(forKey: prefix + "superProperties")
        defaults.synchronize()
    }
    
    // code for unarchiving from legacy archive files and migrating to SQLite / NSUserDefaults persistence
    func migrate() {
        let (eventsQueue,
             peopleQueue,
             groupsQueue,
             superProperties,
             timedEvents,
             distinctId,
             anonymousId,
             userId,
             alias,
             hadPersistedDistinctId,
             peopleDistinctId,
             peopleUnidentifiedQueue,
             optOutStatus,
             automaticEventsEnabled) = unarchive()
        
        saveEntities(eventsQueue, type: PersistenceType.events)
        saveEntities(peopleUnidentifiedQueue, type: PersistenceType.people, flag: true)
        saveEntities(peopleQueue, type: PersistenceType.people)
        saveEntities(groupsQueue, type: PersistenceType.groups)
        saveSuperProperties(superProperties: superProperties)
        saveTimedEvents(timedEvents: timedEvents)
        saveIdentity(distinctID: distinctId, peopleDistinctID: peopleDistinctId, anonymousID: anonymousId, userID: userId, alias: alias, hadPersistedDistinctId: hadPersistedDistinctId)
        if let optOutFlag = optOutStatus {
            saveOptOutStatusFlag(value: optOutFlag)
        }
        if let automaticEventsFlag = automaticEventsEnabled {
            saveAutomacticEventsEnabledFlag(value: automaticEventsFlag, fromDecide: false) // should fromDecide = false here?
        }
        return
    }
    
    private func filePathWithType(_ type: String) -> String? {
        let filename = "mixpanel-\(apiToken)-\(type)"
        let manager = FileManager.default

        #if os(iOS)
            let url = manager.urls(for: .libraryDirectory, in: .userDomainMask).last
        #else
            let url = manager.urls(for: .cachesDirectory, in: .userDomainMask).last
        #endif // os(iOS)
        guard let urlUnwrapped = url?.appendingPathComponent(filename).path else {
            return nil
        }

        return urlUnwrapped
    }
    
    private func unarchive() -> (eventsQueue: Queue,
                                            peopleQueue: Queue,
                                            groupsQueue: Queue,
                                            superProperties: InternalProperties,
                                            timedEvents: InternalProperties,
                                            distinctId: String,
                                            anonymousId: String?,
                                            userId: String?,
                                            alias: String?,
                                            hadPersistedDistinctId: Bool?,
                                            peopleDistinctId: String?,
                                            peopleUnidentifiedQueue: Queue,
                                            optOutStatus: Bool?,
                                            automaticEventsEnabled: Bool?) {
        let eventsQueue = unarchiveEvents()
        let peopleQueue = unarchivePeople()
        let groupsQueue = unarchiveGroups()
        let optOutStatus = unarchiveOptOutStatus()

        let (superProperties,
            timedEvents,
            distinctId,
            anonymousId,
            userId,
            alias,
            hadPersistedDistinctId,
            peopleDistinctId,
            peopleUnidentifiedQueue,
            automaticEventsEnabled) = unarchiveProperties()
        
        if let eventsFile = filePathWithType(PersistenceType.events.rawValue) {
            removeArchivedFile(atPath: eventsFile)
        }
        if let peopleFile = filePathWithType(PersistenceType.people.rawValue) {
            removeArchivedFile(atPath: peopleFile)
        }
        if let groupsFile = filePathWithType(PersistenceType.groups.rawValue) {
            removeArchivedFile(atPath: groupsFile)
        }
        if let propsFile = filePathWithType("properties") {
            removeArchivedFile(atPath:propsFile)
        }
        if let optOutFile = filePathWithType("optOutStatus") {
            removeArchivedFile(atPath: optOutFile)
        }
        if let codelessFile = filePathWithType("codelessBindings") {
            removeArchivedFile(atPath: codelessFile)
        }
        if let variantsFile = filePathWithType("variants") {
            removeArchivedFile(atPath: variantsFile)
        }
        
        return (eventsQueue,
                peopleQueue,
                groupsQueue,
                superProperties,
                timedEvents,
                distinctId,
                anonymousId,
                userId,
                alias,
                hadPersistedDistinctId,
                peopleDistinctId,
                peopleUnidentifiedQueue,
                optOutStatus,
                automaticEventsEnabled)
    }

    private func unarchiveWithFilePath(_ filePath: String) -> Any? {
        if #available(iOS 11.0, macOS 10.13, watchOS 4.0, tvOS 11.0, *) {
            guard let data = try? Data(contentsOf: URL(fileURLWithPath: filePath)),
                  let unarchivedData = try? NSKeyedUnarchiver.unarchiveTopLevelObjectWithData(data)  else {
                Logger.info(message: "Unable to read file at path: \(filePath)")
                removeArchivedFile(atPath: filePath)
                return nil
            }
            return unarchivedData
        } else {
            guard let unarchivedData = NSKeyedUnarchiver.unarchiveObject(withFile: filePath) else {
                Logger.info(message: "Unable to read file at path: \(filePath)")
                removeArchivedFile(atPath: filePath)
                return nil
            }
            return unarchivedData
        }
    }

    private func removeArchivedFile(atPath filePath: String) {
        do {
            try FileManager.default.removeItem(atPath: filePath)
        } catch let err {
            Logger.info(message: "Unable to remove file at path: \(filePath), error: \(err)")
        }
    }

    private func unarchiveEvents() -> Queue {
        let data = unarchiveWithType(PersistenceType.events.rawValue)
        return data as? Queue ?? []
    }

    private func unarchivePeople() -> Queue {
        let data = unarchiveWithType(PersistenceType.people.rawValue)
        return data as? Queue ?? []
    }

    private func unarchiveGroups() -> Queue {
        let data = unarchiveWithType(PersistenceType.groups.rawValue)
        return data as? Queue ?? []
    }

    private func unarchiveOptOutStatus() -> Bool? {
        return unarchiveWithType("optOutStatus") as? Bool
    }

    private func unarchiveProperties() -> (InternalProperties,
        InternalProperties,
        String,
        String?,
        String?,
        String?,
        Bool?,
        String?,
        Queue,
        Bool?) {
            let properties = unarchiveWithType("properties") as? InternalProperties
            let superProperties =
                properties?["superProperties"] as? InternalProperties ?? InternalProperties()
            let timedEvents =
                properties?["timedEvents"] as? InternalProperties ?? InternalProperties()
            let distinctId =
                properties?["distinctId"] as? String ?? ""
            let anonymousId =
                properties?["anonymousId"] as? String ?? nil
            let userId =
                properties?["userId"] as? String ?? nil
            let alias =
                properties?["alias"] as? String ?? nil
            let hadPersistedDistinctId =
                properties?["hadPersistedDistinctId"] as? Bool ?? nil
            let peopleDistinctId =
                properties?["peopleDistinctId"] as? String ?? nil
            let peopleUnidentifiedQueue =
                properties?["peopleUnidentifiedQueue"] as? Queue ?? Queue()
            let automaticEventsEnabled =
                properties?["automaticEvents"] as? Bool ?? nil
        
//            we dont't need to worry about this during migration right?
//            if properties == nil {
//                (distinctId, peopleDistinctId, anonymousId, userId, alias, hadPersistedDistinctId) = restoreIdentity(token: token)
//            }

            return (superProperties,
                    timedEvents,
                    distinctId,
                    anonymousId,
                    userId,
                    alias,
                    hadPersistedDistinctId,
                    peopleDistinctId,
                    peopleUnidentifiedQueue,
                    automaticEventsEnabled)
    }

    private func unarchiveWithType(_ type: String) -> Any? {
        let filePath = filePathWithType(type)
        guard let path = filePath else {
            Logger.info(message: "bad file path, cant fetch file")
            return nil
        }

        guard let unarchivedData = unarchiveWithFilePath(path) else {
            Logger.info(message: "can't unarchive file")
            return nil
        }

        return unarchivedData
    }
}
