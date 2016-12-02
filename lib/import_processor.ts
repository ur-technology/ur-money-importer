/// <reference path="../typings/index.d.ts" />

import * as firebase from 'firebase';
import * as _ from 'lodash';
import * as log from 'loglevel';

export class ImportProcessor {
  env: any;
  db: any;
  importBatchId: string;
  sponsorUserIdsQueue: any[] = [];
  existingUsers: any;
  candidates: any;
  numUsersProcessed: number;
  importRound: number;
  doneLoading: boolean;

  constructor(env: any) {
    this.db = firebase.database();
    this.env = env;
  }

  start(): Promise<any> {
    return new Promise((resolve, reject) => {
      let self = this;

      self.doneLoading = false;

      self.db.ref('/users').once('value', (snapshot: firebase.database.DataSnapshot) => {
        self.existingUsers = snapshot.val() || {};
        _.each(self.existingUsers, (u,uid) => { u.userId = uid; });
        log.info(`${_.size(self.existingUsers)} preexisting users loaded`);
        self.loadCandidatesFromPrefinery(1).then(() => {
          resolve();
        }, (error: any) => {
          self.showStats();
          reject(error);
        });
      }, (error: any) => {
        self.showStats();
        reject(error);
      });
    });
  }

  private showStats() {
    log.info(`  ${_.size(this.candidates)} total candidates processed`);
    _.each(_.groupBy(this.candidates, 'importStatus'), (group, importStatus) => {
      let suffix = '';
      if (importStatus === 'skipped-for-sponsor-email-lookup-failure') {
        let count = _.size(_.uniq(_.map(group, (c: any) => { return c.prefineryUser && c.prefineryUser.referredBy; })));
        suffix = `(${count} unique sponsor emails)`;
      }
      log.info(`    ${_.size(group)} candidates ${importStatus} ${suffix}`);
    });
  }

  private importCandidate(candidate: any): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {

      if (_.isEmpty(_.trim(candidate.email || ''))) {
        candidate.importStatus = 'skipped-for-missing-sponsor-email';
        resolve(false);
        return;
      }

      if (_.some(self.existingUsers, (u: any) => { return u.email === candidate.email })) {
        candidate.importStatus = 'skipped-for-duplicate-email';
        resolve(false);
        return;
      }

      if (_.some(self.existingUsers, (u: any) => { return u.prefineryUser && candidate.prefineryUser && u.prefineryUser.id === candidate.prefineryUser.id; })) {
        candidate.importStatus = 'skipped-for-duplicate-prefinery-id';
        resolve(false);
        return;
      }

      let sponsor = _.find(self.existingUsers, (u: any) => { return u.email === candidate.prefineryUser.referredBy; });
      if (!sponsor) {
        candidate.importStatus = 'skipped-for-sponsor-email-lookup-failure';
        resolve(false);
      }

      candidate.sponsor = {
        userId: sponsor.userId,
        announcementTransactionConfirmed: !!sponsor.wallet &&
          !!sponsor.wallet.announcementTransaction &&
          !!sponsor.wallet.announcementTransaction.blockNumber &&
          !!sponsor.wallet.announcementTransaction.hash
      };
      if (!_.isEmpty(sponsor.name || '')) {
        candidate.sponsor.name = sponsor.name;
      }
      if (!_.isEmpty(sponsor.profilePhotoUrl || '')) {
        candidate.sponsor.profilePhotoUrl = sponsor.profilePhotoUrl;
      }

      if (_.isNumber(sponsor.downlineLevel)) {
        candidate.downlineLevel = sponsor.downlineLevel + 1;
      }
      // add candidate to /users collection
      self.db.ref(`/users/${candidate.userId}`).set(_.omit(candidate,['importStatus', 'userId'])).then(() => {

        // add candidate to sponsor's downline users
        return self.db.ref(`/users/${sponsor.userId}/downlineUsers/${candidate.userId}`).set({
          name: candidate.name,
          profilePhotoUrl: candidate.profilePhotoUrl
        });
      }).then(() => {
        candidate.importStatus = 'imported';
        self.existingUsers[candidate.userId] = candidate;
        _.each(self.candidates, (c) => {
          if (c.importStatus === 'skipped-for-sponsor-email-lookup-failure' && c.prefineryUser && c.prefineryUser.referredBy === candidate.email) {
            c.importStatus = 'unprocessed';
          }
        });
        resolve(true);
      }, (error: any) => {
        reject(error);
      });
    });
  }

  private importUnprocessedCandidates(): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {
      self.importRound++;

      let numCandidates = _.size(self.candidates);
      let randomCandidate = _.sample(self.candidates);
      let unprocessedCandidates: any = _.pickBy(self.candidates, (c: any): boolean => { return c.importStatus === 'unprocessed'; });
      let numRecordsRemaining = _.size(unprocessedCandidates);
      if (numRecordsRemaining === 0) {
        resolve();
        return;
      }

      log.info(`  starting round ${self.importRound} of imports...`);
      let finalized = false;
      _.each(unprocessedCandidates, (candidate) => {
        self.importCandidate(candidate).then((imported) => {
          numRecordsRemaining--;
          if (!finalized && numRecordsRemaining === 0) {
            self.importUnprocessedCandidates().then(() => {
              if (!finalized) {
                finalized = true;
                resolve();
              }
            }, (error) => {
              if (!finalized) {
                finalized = true;
                reject(error);
              }
            });
          }
        }, (error) => {
          if (!finalized) {
            finalized = true;
            reject(error);
          }
        });

      });
    });
  }

  private loadCandidatesFromPrefinery(startPage: number): Promise<any> {
    let self = this;
    self.candidates = self.candidates || {};
    self.importBatchId = self.importBatchId || self.db.ref("/users").push().key; // HACK to generate unique id

    return new Promise((resolve, reject) => {
      var request = require('request');
      let finalized: boolean = false;
      let GROUP_SIZE = 1;
      let numPagesRemaining = GROUP_SIZE;
      let emptyPageEncountered = false;

      log.info(`requesting pages ${startPage} through ${startPage + GROUP_SIZE - 1} of testers from prefinery...`);
      for (let index = 0; index < GROUP_SIZE; index++) {
        let page = startPage + index;
        try {
          let options = {
            url: `https://api.prefinery.com/api/v2/betas/9505/testers.json?api_key=dypeGz4qErzRuN143ZVN2fk2SagFqKPN&page=${page}`,
            method: 'GET',
            headers: { 'Content-Type': 'application/json' },
            body: {},
            json: true
          };
          request(options, (error: any, response: any, prefineryUsers: any) => {
            if (finalized) {
              return;
            }
            if (error) {
              finalized = true;
              reject(`error retrieving data from the prefinery api: ${error}`);
              return;
            }
            _.each(prefineryUsers, (prefineryUser: any, index: number) => {
              if (_.includes(['active', 'applied'],prefineryUser.status)) {
                let candidate: any = self.buildCandidate(prefineryUser);
                self.candidates[candidate.userId] = candidate;
              }
            });
            if (_.isEmpty(prefineryUsers)) {
              emptyPageEncountered = true;
            }
            numPagesRemaining--;
            if (numPagesRemaining === 0) {
              self.importRound = 0;
              self.importUnprocessedCandidates().then(() => {
                self.showStats();
                if (emptyPageEncountered) {
                  return Promise.resolve(undefined);
                } else {
                  return self.loadCandidatesFromPrefinery(startPage + GROUP_SIZE);
                }
              }).then(() => {
                if (!finalized) {
                  finalized = true;
                  resolve();
                }
              }, (error) => {
                reject(error);
              });
            }

          });
        } catch(error) {
          if (!finalized) {
            finalized = true;
            reject(`got error when attempting to get data from prefinery: ${error}`);
          }
        }
      }
    });
  }

  buildCandidate(prefineryUser: any) {
    let processedPrefineryUser: any = {
      importBatchId: this.importBatchId,
      id: prefineryUser.id,
      email: prefineryUser.email,
      ipAddress: prefineryUser.ip_address,
      joinedAt: prefineryUser.joined_at,
      firstName: prefineryUser.profile.first_name,
      lastName: prefineryUser.profile.last_name,
      country: prefineryUser.profile.country,
      phone: prefineryUser.profile.telephone,
      referralLink: prefineryUser.profile.http_referrer,
      referredBy: prefineryUser.referred_by,
      userReportedCountry: (_.find((prefineryUser.responses || []), (r: any) => { return r.question_id === 82186 }) || {}).answer,
      userReportedReferrer: (_.find((prefineryUser.responses || []), (r: any) => { return r.question_id === 82178 }) || {}).answer
    };
    processedPrefineryUser = _.mapValues(_.omitBy(processedPrefineryUser, _.isNil), (value: string) => { return _.trim(value || ''); });
    let candidate: any = {
      "firstName" : _.startCase(_.toLower(prefineryUser.profile.first_name)),
      "lastName" : _.startCase(_.toLower(prefineryUser.profile.last_name)),
      "email" : _.toLower(_.trim(prefineryUser.email || '')),
      "prefineryUser" : processedPrefineryUser,
      "createdAt" : firebase.database.ServerValue.TIMESTAMP
    };
    candidate.name = `${candidate.firstName} ${candidate.lastName}`;
    candidate.profilePhotoUrl = this.generateProfilePhotoUrl(candidate);
    candidate.importStatus = 'unprocessed';
    candidate.userId = this.db.ref("/users").push().key;
    return candidate;
  }

  shutdown(): Promise<any> {
    return new Promise((resolve, reject) => {
      resolve();
    });
  }

  generateProfilePhotoUrl(user: any) {
    let colorScheme = _.sample([{
      background: "DD4747",
      foreground: "FFFFFF"
    }, {
      background: "ED6D54",
      foreground: "FFFFFF"
    }, {
      background: "FFBE5B",
      foreground: "FFFFFF"
    }, {
      background: "FFE559",
      foreground: "FFFFFF"
    }, {
      background: "e9F0A1",
      foreground: "000000"
    }, {
      background: "C79DC7",
      foreground: "000000"
    }, {
      background: "8ADED7",
      foreground: "000000"
    }, {
      background: "A1EDA1",
      foreground: "000000"
    }]);
    let initials = 'XX';
    if (user.firstName) {
      let firstLetters = user.firstName.match(/\b\w/g);
      initials = firstLetters ? firstLetters[0] : 'X';
      let lastNameFirstLetters = (user.lastName || '').match(/\b\w/g);
      if (lastNameFirstLetters) {
        initials = initials + lastNameFirstLetters[0];
      }
      initials = initials.toUpperCase();
    }
    return "https://dummyimage.com/100x100/" + colorScheme.background + "/" + colorScheme.foreground + "&text=" + initials;
  };

  private doBlast() {
    let self = this;
    let messageName: string = "updated-url";
    self.db.ref("/users").orderByChild("invitedAt").on("child_added", (userSnapshot: firebase.database.DataSnapshot) => {
      let user: any = userSnapshot.val();
      let alreadySent: boolean = _.some(user.smsMessages, (message: any, messageId: string) => { return message.name === messageName; });
      if (alreadySent) {
        return;
      }

      let text: string;
      if (self.isCompletelySignedUp(user)) {
        text = "Thanks again for taking part in the UR Capital beta program! In the coming weeks, we’ll be releasing our new, free mobile app—UR Money—aimed at making it easier for non-technical people to acquire and use cryptocurrency for everyday transactions. As a beta tester, you will be awarded an amount of cryptocurrency based on the status you build by referring others to the beta test. We look forward to welcoming you to the world of cryptocurrency!";
      } else {
        text = "This is a reminder that name has invited you to take part in the UR Capital beta test. There are only a few weeks left to sign up. As a beta tester, you will be the first to access UR Money, a free mobile app that makes it easier for non-technical people to acquire and use cryptocurrency for everyday transactions. You will also be awarded an amount of cryptocurrency based on the status you build by referring others to the beta test. We look forward to welcoming you to the world of cryptocurrency!";
      }
      text = text + " put url here";
      userSnapshot.ref.child("smsMessages").push({
        name: messageName,
        type: self.isCompletelySignedUp(user) ? "signUp" : "invitation",
        createdAt: firebase.database.ServerValue.TIMESTAMP,
        sendAttempted: false,
        phone: user.phone,
        text: text
      });
    });
  }

  private fixUserData() {
    let self = this;
    self.db.ref("/users").on("child_added", (userSnapshot: firebase.database.DataSnapshot) => {
      let user = userSnapshot.val();
      let userId = userSnapshot.key;
      self.traverseObject(`/users/${userId}`, user, (valuePath: string, value: any, key: string) => {
        if (_.isObject(value) && value.firstName && /dummyimage/.test(value.profilePhotoUrl)) {
          let ref = self.db.ref(valuePath);
          log.info(`about to update value at ${valuePath}, firstName=${value.firstName}`);
          ref.update({ profilePhotoUrl: self.generateProfilePhotoUrl(value) });
        }
      });
    });
  }

  private traverseObject(parentPath: string, object: any, callback: any) {
    _.forEach(object, (value, key) => {
      let currentPath: string = `${parentPath}/${key}`;
      callback(currentPath, value, key);
      if (_.isObject(value) || _.isArray(value)) {
        this.traverseObject(currentPath, value, callback);
      }
    });
  }

  lookupUsersByPhone(phone: string): Promise<any[]> {
    let ref = this.db.ref("/users").orderByChild("phone").equalTo(phone);
    return this.lookupUsers(ref);
  }


  private lookupUserByEmail(email: string): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {
      self.lookupUsersByEmail(email).then((matchingUsers) => {

        if (_.isEmpty(matchingUsers)) {
          log.info(`no matching user found for ${email}`);
          resolve(undefined);
          return;
        }

        let activeUsers = _.reject(matchingUsers, 'disabled');
        if (_.isEmpty(activeUsers)) {
          let disabledUser: any = _.first(matchingUsers);
          log.info(`found matching user ${disabledUser.userId} for ${email} but user was disabled`);
          resolve(undefined);
          return;
        }
        if (_.size(activeUsers) > 1) {
          reject(`more than one active user found for ${email}`);
          return;
        }

        let matchingUser: any = _.first(activeUsers);
        // log.debug(`matching user with userId ${matchingUser.userId} found for ${email}`);
        resolve(matchingUser);
      }, (error) => {
        reject(error);
      });
    });
  }

  lookupUsersByEmail(email: string): Promise<any[]> {
    let ref = this.db.ref("/users").orderByChild("email").equalTo(email);
    return this.lookupUsers(ref);
  }

  lookupUsers(ref: any): Promise<any[]> {
    let self = this;
    return new Promise((resolve, reject) => {
      ref.once("value", (snapshot: firebase.database.DataSnapshot) => {

        // sort matching users with most completely signed up users first
        let userMapping = snapshot.val() || {};
        let users = _.values(userMapping);
        let userIds = _.keys(userMapping);
        _.each(users, (user: any, index: number) => { user.userId = userIds[index]; });
        let sortedUsers = _.reverse(_.sortBy(users, (user) => { return self.completenessRank(user); }));
        let sortedUserIds = _.map(sortedUsers, (user) => { return user.userId });

        resolve(sortedUsers);
      }, (error: string) => {
        reject(error);
      });
    });
  };


  lookupUserById(userId: string): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {
      let userRef = self.db.ref(`/users/${userId}`);
      userRef.once('value', (snapshot: firebase.database.DataSnapshot) => {
        let user: any = snapshot.val();
        if (user) {
          user.userId = userId;
          resolve(user);
        } else {
          let error = `no user exists at location ${userRef.toString()}`
          log.warn(error);
          reject(error);
        }
      });
    });
  }

  registrationStatus(user: any): string {
    return _.trim((user && user.registration && user.registration.status) || "initial");
  }

  verificationCompleted(user: any) {
    return _.includes([
        'verification-succeeded',
        'announcement-requested',
        'announcement-failed',
        'announcement-succeeded'
      ], this.registrationStatus(user));
  }

  isCompletelySignedUp(user: any) {
    return !user.disabled &&
      this.verificationCompleted(user),
      !!user.name &&
      !!user.wallet && !!user.wallet.address;
  }

  private numberToHexString(n: number) {
    return "0x" + n.toString(16);
  }

  private hexStringToNumber(hexString: string) {
    return parseInt(hexString, 16);
  }

  private completenessRank(user: any) {
    return (this.verificationCompleted(user) ? 1000 : 0) +
     (user.wallet && !!user.wallet.address ? 100 : 0) +
     (user.name ? 10 : 0) +
     (user.profilePhotoUrl ? 1 : 0);
  }

  private containsUndefinedValue(objectOrArray: any): boolean {
    return _.some(objectOrArray, (value, key) => {
      let type = typeof (value);
      if (type === 'undefined') {
        return true;
      } else if (type === 'object') {
        return this.containsUndefinedValue(value);
      } else {
        return false;
      }
    });
  }



  start2(): Promise<any> {
    return new Promise((resolve, reject) => {
      let self = this;
      let usersSearch = self.db.ref(`/users`).orderByChild('email');
      usersSearch.once('value').then((snapshot: firebase.database.DataSnapshot) => {
        self.existingUsers = snapshot.val();
        log.info(`retrieved ${_.size(self.existingUsers)} users from db`);
        _.each(self.existingUsers, (u,uid) => {
          u.userId = uid;
        });
        self.calculateDownlineSizeForAllUsers();
      //   return self.checkForCircularRefs();
      // }).then(() => {
      //   return self.correctSponsorInfoForAllUsersInQueue(true);
      // }).then(() => {
        resolve();
      }, (error: any) => {
        reject(error);
      });
    });
  }

  calculateDownlineSizeForAllUsers() {
    let self = this;
    let startingUser = _.find(self.existingUsers, (u: any, uid: string) => {
      return u.email === 'jreitano@ur.technology';
    });
    self.numUsersProcessed = 0;
    self.calculateDownlineSizeForOneUser(startingUser, true);

    let previouslyProcessedUsers: any = self.existingUsers;
    while (true) {
      let unprocessedUsers: any = _.filter(previouslyProcessedUsers, (u: any) => {
        return _.isNil(u.downlineSize);
      });
      if (_.isEmpty(unprocessedUsers)) {
        break;
      }
      log.info(`${_.size(unprocessedUsers)} users still unprocessedx`);
      let user: any = _.sample(unprocessedUsers);
      self.calculateDownlineSizeForOneUser(user, false);
      previouslyProcessedUsers = unprocessedUsers;
    }

    let userIds: string[] = <string[]> _.keys(self.existingUsers);
    let userIdsSortedByDownlineSize: string[] = <string[]> _.sortBy(userIds, (userId: string) => {
      let user = self.existingUsers[userId];
      if (!user) {
        log.warn(`could not find user for userId ${userId}`);
        throw `could not find user for userId ${userId}`;
      }
      if (_.isNil(user.downlineSize)) {
        log.warn(`could not find downline size for userId`, user);
        throw `could not find downline size for userId ${userId}`;
      }
      return 1000000 - user.downlineSize;
    });

    let stringify = require('csv-stringify');
    let data = <string[][]> _.map(userIdsSortedByDownlineSize, (userId: string) => {
      let user = self.existingUsers[userId];
      let prefineryUser: any = user.prefineryUser || user.prefinery || {};
      return [
        prefineryUser.id || '',
        user.email || '',
        user.firstName || '',
        user.lastName || '',
        user.name || '',
        prefineryUser.country || '',
        user.phone || prefineryUser.phone || '',
        prefineryUser.joinedAt || '',
        prefineryUser.ipAddress || '',
        prefineryUser.referralLink || '',
        prefineryUser.referredBy || '',
        prefineryUser.httpReferrer || '',
        prefineryUser.userReportedCountry || '',
        prefineryUser.userReportedReferrer || '',
        prefineryUser.referredBy2 || prefineryUser.sponsorEmail || '',
        user.userId || '',
        '' + user.downlineSize,
        '' + user.numDirectReferrals,
      ];
    });
    _.each(data, (row) => {
      let quotedFields = _.map(row, (r) => { return `\"${r}\"`; });
      log.info(_.join(quotedFields,"\t"));
    });
  }

  calculateDownlineSizeForOneUser(user: any, quickSearch: boolean) {
    let self = this;
    if (user.downlineSize !== undefined) {
      return;
    }

    let directReferrals: any[];
    if (quickSearch) {
      directReferrals = _.map(user.downlineUsers, (u: any, uid: string) => {
       return self.existingUsers[uid];
     });
    } else {
      directReferrals = _.filter(self.existingUsers, (u: any, uid: string) => {
         return u.sponsor && u.sponsor.userId == user.userId;
      });
    }
    user.numDirectReferrals = directReferrals.length;
    user.downlineSize = user.numDirectReferrals;
    _.each(directReferrals, (referralUser,referralUserId) => {
      self.calculateDownlineSizeForOneUser(referralUser, quickSearch);
      user.downlineSize = user.downlineSize + referralUser.downlineSize;
    });
    if (user.downlineSize > 1000) {
      log.info(`user ${user.userId} has downline size of ${user.downlineSize}`);
    }
    self.numUsersProcessed++;
  }

  correctSponsorInfoForAllUsersInQueue(firstTime?: boolean): Promise<any> {
    return new Promise((resolve, reject) => {
      let self = this;

      if (firstTime) {
        let startingUser = _.find(self.existingUsers, (u: any, uid: string) => {
          return u.email === 'jreitano@ur.technology';
        });
        self.sponsorUserIdsQueue.push(startingUser.userId);
        _.each(self.existingUsers, (u,uid) => {
          u.processed = false;
        });
        self.numUsersProcessed = 0;
      }

      if (_.isEmpty(self.sponsorUserIdsQueue)) {
        resolve();
        return;
      }

      self.correctSponsorInfoForFirstUserInQueue().then(() => {
        return self.correctSponsorInfoForAllUsersInQueue();
      }).then(() => {
        if (firstTime) {
          let missedUsers = _.reject(self.existingUsers, 'processed');
          log.info('missed users', missedUsers);
        }
        resolve();
      }, (error: any) => {
        reject(error);
      });
    });
  }

  correctSponsorInfoForFirstUserInQueue(): Promise<any> {
    return new Promise((resolve, reject) => {
      let self = this;

      let sponsorUserId = self.sponsorUserIdsQueue.shift();
      let sponsor = self.existingUsers[sponsorUserId];
      if (sponsor.processed) {
        reject('unexpectedly processed 1');
        return;
      }

      let directReferrals = _.filter(self.existingUsers, (u: any, uid: string) => {
        return u.sponsor && u.sponsor.userId === sponsor.userId;
      });

      let numRecordsLeft: number = _.size(directReferrals);
      let finalized = false;

      function resolveIfDoneWithDirectReferrals() {
        if (!finalized && numRecordsLeft === 0) {

          if (sponsor.processed) {
            reject('unexpectedly processed 2');
            return;
          }
          sponsor.processed = true;
          self.numUsersProcessed++;
          if (self.numUsersProcessed % 1000 === 0) {
            log.info(`numUsersProcessed=${self.numUsersProcessed}`)
          }

          finalized = true;
          resolve();
        }
      }

      self.db.ref(`/users/${sponsorUserId}/disabled`).remove().then(() => {
        let downlineUsers: any = {};
        _.each(directReferrals, (user: any) => {
          downlineUsers[user.userId] = _.omit(_.pick(user, ['name', 'profilePhotoUrl']),_.isNil);
        });
        if (_.isEmpty(downlineUsers)) {
          return self.db.ref(`/users/${sponsorUserId}/downlineUsers`).remove();
        } else {
          return self.db.ref(`/users/${sponsorUserId}/downlineUsers`).set(downlineUsers);
        }
      }).then(() => {
        resolveIfDoneWithDirectReferrals();
        _.each(directReferrals, (user: any) => {
          self.db.ref(`/users/${user.userId}`).update({downlineLevel: sponsor.downlineLevel + 1}).then(() => {
            return self.db.ref(`/users/${user.userId}/sponsor`).update({
              name: sponsor.name,
              profilePhotoUrl: sponsor.profilePhotoUrl,
              announcementTransactionConfirmed: !!sponsor.wallet &&
                !!sponsor.wallet.announcementTransaction &&
                !!sponsor.wallet.announcementTransaction.blockNumber &&
                !!sponsor.wallet.announcementTransaction.hash
            });
          }).then(() => {
            self.sponsorUserIdsQueue.push(user.userId);
            numRecordsLeft--;
            resolveIfDoneWithDirectReferrals();
          }, (error: any) => {
            if (!finalized) {
              finalized = true;
              reject(error);
            }
          });
        });
      });
    });
  }

  checkForCircularRefs(): Promise<any> {
    return new Promise((resolve, reject) => {
      let self = this;

      // first make sure all users descended from jreitano@ur.technology and sponsor user id is set correctly
      let malformedUsers = _.filter(self.existingUsers, (user: any, userId: string) => {
        if (user.email === 'jreitano@ur.technology') {
          return false;
        } else if (!user.sponsor || !user.sponsor.userId) {
          return true;
        } else if (userId === user.sponsor.userId) {
          return true;
        } else if (!self.existingUsers[user.sponsor.userId]) {
          return true;
        } else {
          return false
        }
      });
      if (!_.isEmpty(malformedUsers)) {
        log.info('malformedUsers=', malformedUsers);
        reject(`found malformedUser ${malformedUsers[0]} with id ${malformedUsers[0].userId}`);
        return;
      }

      let numRecordsLeft: number = _.size(self.existingUsers);
      let circularRefUserIds: any = {};
      let finalized: boolean = false;
      _.each(self.existingUsers, (user: any, userId: string) => {
        user.processed = false;
        self.checkForCircularRefsByOneUser(user, userId).then(() => {
          user.processed = true;
          numRecordsLeft--;
          if (!finalized && numRecordsLeft === 0) {
            finalized = true;
            _.each(circularRefUserIds, (_,uid) => {
              let url = self.db.ref(`/users/${uid}`).toString();
              log.info(`circular reference by user ${url}`);
            });
            let unprocessedUsers = _.map(_.reject(self.existingUsers, 'processed'),(u) => {return u;});
            if (!_.isEmpty(unprocessedUsers)) {
              log.info(`unprocessedUsers=`,unprocessedUsers);
            }
            resolve();
          }
        }, (error: any) => {
          if (!finalized) {
            finalized = true;
            reject(error);
          }
        });
      });
    });
  }

  checkForCircularRefsByOneUser(startingUser: any, startingUserId: string): Promise<any> {
    return new Promise((resolve, reject) => {
      let self = this;

      let user = startingUser;
      let userId = startingUserId;
      let alreadyEncounteredUserIds: string[] = [];

      while (true) {
        if (user.email === 'jreitano@ur.technology') {
          resolve()
          break;
        }
        let alreadyEncounteredUserId = _.find(alreadyEncounteredUserIds, (uid,index) => {
          return uid === userId;
        });
        if (alreadyEncounteredUserId) {
          reject(`found circular references for user ids ${alreadyEncounteredUserIds}`);
          break;
        }
        alreadyEncounteredUserIds.push(userId);
        let sponsorId = user.sponsor.userId;
        user = self.existingUsers[sponsorId];
        userId = sponsorId;
      }
    });
  }

  checkDownlineLevel(user: any, userId: string): Promise<any> {
    return new Promise((resolve, reject) => {
      let self = this;

      let userRef = self.db.ref(`/users/${userId}`);
      let userPath = userRef.toString();

      if (!user.sponsor || !user.sponsor.userId) {
        log.info(`user ${userPath} lacks sponsor`);
        resolve(false);
        return;
      }

      let sponsorSearchRef = self.db.ref(`/users/${user.sponsor.userId}`);
      sponsorSearchRef.once('value').then((snapshot: firebase.database.DataSnapshot) => {

        if (!snapshot.exists()) {
          log.info(`can't find sponsor ${user.sponsor.userId} for user ${userPath}`);
          resolve(false);
          return;
        }

        let sponsor: any = snapshot.val();
        let sponsorUserId: string = snapshot.key;

        if (userId === sponsorUserId) {
          log.info(`user ${userPath} is his own sponsor`);
          resolve(false);
          return;
        }

        if (!_.isNil(user.downlineLevel) && !_.isNumber(user.downlineLevel)) {
          log.info(`user ${userPath} has non-numeric downline level ${user.downlineLevel}`);
          resolve(false);
          return;
        }

        if (_.isNil(sponsor.downlineLevel)) {
          log.info(`sponsor ${sponsorSearchRef.toString()} missing downline level`);
          resolve(false);
          return;
        }

        if (sponsor.downlineLevel === 0 || sponsor.downlineLevel === 1) {
          log.info(`sponsor ${sponsorSearchRef.toString()} has downline level ${sponsor.downlineLevel}`);
          resolve(false);
          return;
        }

        if (!_.isNumber(sponsor.downlineLevel)) {
          log.info(`sponsor ${sponsorSearchRef.toString()} has non-numeric downline level ${sponsor.downlineLevel}`);
          resolve(false);
          return;
        }

        if (_.isNil(user.downlineLevel)) {
          userRef.update({downlineLevel: sponsor.downlineLevel + 1}).then(() => {
            log.info(`updated user ${userPath} with downlineLevel ${sponsor.downlineLevel + 1}`);
            resolve(true);
          });
        } else {
          if (sponsor.downlineLevel >= user.downlineLevel) {
            log.info(`sponsor ${sponsorSearchRef.toString()} has greater downline level than user ${userPath}`);
            resolve(false);
            return;
          }

          resolve(true);
        }
      });
    });
  }

}
