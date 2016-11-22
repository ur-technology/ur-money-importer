/// <reference path="../typings/index.d.ts" />

import * as firebase from 'firebase';
import * as _ from 'lodash';
import * as log from 'loglevel';

export class ImportProcessor {
  env: any;
  db: any;
  _web3: any;
  newUsers: any[] = [];
  sponsorsQueue: any[];
  numImportedUsers: number = 0;

  web3() {
    if (!this._web3) {
      let Web3 = require('web3');
      this._web3 = new Web3();
      this._web3.setProvider(new this._web3.providers.HttpProvider("http://127.0.0.1:9595"));
    }
    return this._web3;
  }

  constructor(env: any) {
    this.db = firebase.database();
    this.env = env;
  }

  start(): Promise<any> {
    return new Promise((resolve, reject) => {
      let self = this;
      self.db.ref('/importedUsers').remove().then(() => {
        return self.buildPrefineryUsers();
      }).then(() => {
        log.info(`numNewUsers=${_.size(self.newUsers)}`);
        return self.db.ref(`/users`).once("value");
      }).then((snapshot: firebase.database.DataSnapshot) => {
        self.sponsorsQueue = [];
        let userMapping = snapshot.val();
        _.each(userMapping, (user, userId) => {
          if (user.email) {
            user.userId = userId;
            self.sponsorsQueue.push(user);
          }
        });
        let sponsorsWithoutDownlineLevel = _.reject(self.sponsorsQueue, (user) => {return !!user.downlineLevel;});
        if (_.size(sponsorsWithoutDownlineLevel) > 0) {
          log.info(`number of sponsorsWithoutDownlineLevel=${_.size(sponsorsWithoutDownlineLevel)}`);
          _.each(sponsorsWithoutDownlineLevel, (user) => {
            console.log(user.email);
          });
        }
        return self.processAllSponsors();
      }).then(() => {
        let numNotImportedUsers = _.size(self.newUsers) - self.numImportedUsers;
        log.info(`${_.size(self.newUsers)} total users; ${self.numImportedUsers} imported; ${numNotImportedUsers} not imported`);

        let newUsersWithoutUserId = _.reject(self.newUsers, (newUser) => { return newUser.userId; });
        if (_.size(newUsersWithoutUserId) > 0) {
          log.info(`num newUsersWithoutUserId=${_.size(newUsersWithoutUserId)}`);
          _.each(newUsersWithoutUserId, (u) => { log.info(u.email); });
        }
        resolve();
      }, (error: any) => {
        reject(error);
      });
    });
  }

  processAllSponsors(): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {
      self.processFirstSponsor().then((moreInQueue: boolean) => {
        if (!moreInQueue) {
          resolve();
          return;
        }
        return self.processAllSponsors();
      }).then(() => {
        resolve();
      }, (error: any) => {
        reject(error);
      });
    });
  }

  processFirstSponsor(): Promise<any> {
    return new Promise((resolve, reject) => {
      let self = this;

      let sponsor = self.sponsorsQueue.shift();
      if (!sponsor) {
        resolve(false);
        return;
      }

      let invitees = _.filter(self.newUsers, (newUser) => {
        return newUser.prefineryUser.sponsorEmail === sponsor.email;
      });
      let numInviteesRemaining = _.size(invitees);

      log.info(`${_.size(self.sponsorsQueue)} in queue - plus found ${numInviteesRemaining} invitees of user ${sponsor.email}`);

      if (numInviteesRemaining == 0) {
        resolve(true);
        return;
      }

      let finalized = false;
      _.each(invitees, (invitee) => {
        invitee.sponsor = _.pick(sponsor, ['userId', 'name', 'profilePhotoUrl']);
        invitee.downlineLevel = sponsor.downlineLevel + 1;
        let usersRef = self.db.ref('/importedUsers');
        let inviteeUserId = usersRef.push().key;
        usersRef.child(inviteeUserId).set(invitee).then(() => {
          log.info(`imported user ${invitee.email}`);
          invitee.userId = inviteeUserId;
          self.sponsorsQueue.push(invitee);
          self.numImportedUsers++;
          numInviteesRemaining--;
          if (!finalized && numInviteesRemaining == 0) {
            resolve(true);
            finalized = true;
          }
        }, (error: any) => {
          numInviteesRemaining--;
          if (!finalized) {
            reject(error);
            finalized = true;
          }
        });
      });
    });
  }

  buildPrefineryUsers(): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {
      self.db.ref(`/prefineryData`).once("value", (snapshot: firebase.database.DataSnapshot) => {
        let prefineryUsers: any = snapshot.val();
        _.each(prefineryUsers, (prefineryUser, importIndex) => {
          prefineryUser.importIndex = importIndex;
          let firstName = _.startCase(_.toLower(prefineryUser.firstName));
          let lastName = _.startCase(_.toLower(prefineryUser.lastName));
          let user: any = {
            "firstName" : firstName,
            "lastName" : lastName,
            "name" : `${firstName} ${lastName}`,
            "email" : prefineryUser.email,
            "prefineryUser" : prefineryUser
          };
          user.prefineryUser.id = user.prefineryUser.id || user.prefineryUser.Id;
          delete user.prefineryUser.Id;
          user.profilePhotoUrl = self.generateProfilePhotoUrl(user);
          self.newUsers.push(user);
        });
        resolve();
      });
    });
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
      let alreadySent: boolean = _.some(user.smsMessages, (message: any, messageId: string) => { return message.name == messageName; });
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
      if (type == 'undefined') {
        return true;
      } else if (type == 'object') {
        return this.containsUndefinedValue(value);
      } else {
        return false;
      }
    });
  }

}
