import * as firebase from 'firebase';
import * as _ from 'lodash';
import * as log from 'loglevel';
import {BigNumber} from 'bignumber.js';
import {sprintf} from 'sprintf-js';
import {ManualIDVerifier} from './manual';
import * as moment from 'moment';

export class DataManipulator {
  env: any;
  db: any;
  auth: any;
  users: any;
  disabledUsers: any[];
  nonDisabledUsers: any[];
  usersWithBonuses: any[];
  usersWithoutWallets: any[];
  usersWithWalletsButNoBonuses: any[];


  usersWithDocsPending: any[];
  usersWithReviewPending: any[];
  approvedUsersWithAnnouncementPending: any[];
  otherUsers: any[];

  twilioLookupsClient: any;
  twilioRestClient: any;

  constructor(env: any, db: any, auth: any) {
    this.env = env;
    this.db = db;
    this.auth = auth;
    // file smallUsers.json was generated with the following command:
    //   jq '.users | with_entries(del(.value.chats,.value.chatSummaries, .value.transactions, .value.events, .value.registration.verificationArgs, .value.registration.verificationResult))' ./data/ur-money-production_data.json > ./data/smallUsers.json

    this.users = require('../data/smallUsers.json');
    _.each(this.users, (u, userId) => {
      u.userId = userId;
      u.state = this.userState(u);
    });
  }

  private ref(path: string) {
    return this.db.ref(path);
  }

  private userRef(u: any) {
    return this.ref('/users').child(u.userId);
  }

  private getSponsor(u: any) {
    return this.users[u.sponsor && u.sponsor.userId];
  }

  private directReferrals(u: any) {
    return _.filter(this.users, (x: any) => { return x.sponsor && x.sponsor.userId === u.userId; });
  }

  private addDownlineInfoToUpline(u: any) {
    let status = (u.registration && u.registration.status) || 'initial';
    let bonuses = [60.60, 60.60, 121.21, 181.81, 303.03, 484.84, 787.91];

    let targetUser = u;
    for (let level = 0; level < 7; level++) {
      targetUser = targetUser.sponsor && targetUser.sponsor.userId && this.users[targetUser.sponsor.userId];
      if (!targetUser) {
        break;
      }
      if (level === 0) {
        targetUser.d.numDirectReferrals = (targetUser.d.numDirectReferrals || 0) + 1;
      }
      targetUser.d.numDownlineUsers = (targetUser.d.numDownlineUsers || 0) + 1;
      if (!u.disabled && status !== 'announcement-confirmed') {
        targetUser.d.potentialUrRewards = (targetUser.d.potentialUrRewards || 0) + bonuses[level];
      }
    }
  }

  private setSponsorToEiland(u: any) {
    let eiland: any = _.find(this.users, { email: 'eiland@ur.technology' });
    let sponsorInfo: any = _.pick(eiland, ['name', 'profilePhotoUrl', 'userId']);
    sponsorInfo.announcementTransactionConfirmed = true;
    this.userRef(u).update({ sponsor: sponsorInfo });
  }

  private isTopUser(u: any): boolean {
    return u.email === 'jreitano@ur.technology';
  }

  private userState(u: any): string {
    if (!this.isTopUser(u) && !u.sponsor) {
      return 'missing-sponsor';
    } if (u.disabled) {
      return 'disabled'
    } if (u.fraudSuspected) {
      return 'fraud-suspected'
    } else if (u.wallet && u.wallet.announcementTransaction && u.wallet.announcementTransaction.hash && u.wallet.announcementTransaction.blockNumber && u.signUpBonusApproved) {
      return 'bonus-approved';
    } else if (u.wallet && u.wallet.announcementTransaction && u.wallet.announcementTransaction.hash && u.wallet.announcementTransaction.blockNumber) {
      return 'announcement-confirmed';
    } else if (u.wallet && u.wallet.announcementTransaction && u.wallet.announcementTransaction.hash) {
      return 'waiting-for-announcement-to-be-confirmed';
    } else if (!u.serverHashedPassword && !(u.wallet && u.wallet.address)) {
      return 'missing-password-and-wallet';
    } else if (!u.signUpBonusApproved && !(u.idUploaded && u.selfieMatched)) {
      return 'missing-docs';
    } else if (!u.signUpBonusApproved) {
      return 'waiting-for-review'; // need to get further detail from FreshDesk
    } else if (!(u.wallet && u.wallet.address)) {
      return 'missing-wallet';
    } else if (!u.sponsor.announcementTransactionConfirmed) {
      let s = this.sponsorState(u);
      switch (s) {
        case 'missing-sponsor':
        case 'disabled':
        case 'fraud-suspected':
        case 'missing-password-and-wallet':
        case 'missing-docs':
        case 'waiting-for-review':
        case 'missing-wallet':
        case 'blocked-by-upline':
        case 'waiting-for-announcement-to-be-queued':
          return `blocked-by-upline`

        default:
          return `unexpected-sponsor-state: ${s}`;
      }
    } else {
      return 'waiting-for-announcement-to-be-queued';
    }
  }

  private sponsorState(u: any): any {
    let sponsor = this.getSponsor(u);
    return sponsor && this.userState(sponsor);
  }

  private rootUser(u: any): any {
    if (this.userState(u) === 'blocked-by-upline') {
      return this.rootUser(this.getSponsor(u));
    } else {
      return u;
    }
  }

  private rootState(u: any): any {
    return this.userState(this.rootUser(u));
  }

  private blocker(u: any): any {
    if (this.userState(u) !== 'blocked') {
      return undefined;
    }

    let upline = this.uplineUsers(u);
    return _.find(this.uplineUsers(u), (up) => {
      return this.userState(up) !== 'blocked';
    });
  }

  fixDownlineInfo() {
    let self = this;
    let updatedDownlineLevels = 0;
    let updatedDownlineUsers = 0;
    let updatedDownlineSizes = 0;
    _.each(self.users, (u) => {
      u.newDownlineSize = 0;
      u.newDownlineUsers = {};
    });
    _.each(self.users, (u) => {
      let upline: any[] = self.uplineUsers(u);
      u.newDownlineLevel = upline.length + 1;
      if (upline[0]) {
        upline[0].newDownlineUsers[u.userId] = _.pick(u, ['name', 'profilePhotoUrl']);
      }
      _.each(upline, (uplineUser) => { uplineUser.newDownlineSize++; });
    });
    _.each(self.users, (u) => {
      if (u.newDownlineLevel !== u.downlineLevel) {
        u.downlineLevel = u.newDownlineLevel;
        self.userRef(u).update({ downlineLevel: u.downlineLevel });
        updatedDownlineLevels++;
      }

      if (!_.isEqual(u.newDownlineUsers, u.downlineUsers || {})) {
        // log.info(`\n\n***u.userId=${u.userId}\n***u.newDownlineUsers=${JSON.stringify(u.newDownlineUsers)}\n***u.downlineUsers=${JSON.stringify(u.downlineUsers)}`);
        u.downlineUsers = u.newDownlineUsers;
        self.userRef(u).update({ downlineUsers: u.downlineUsers });
        updatedDownlineUsers++;
      }

      if (u.newDownlineSize !== u.downlineSize) {
        // log.info(`\n\n***u.userId=${u.userId}\n***u.newDownlineSize=${u.newDownlineSize}\n***u.downlineSize=${u.downlineSize}`);
        u.downlineSize = u.newDownlineSize;
        self.userRef(u).update({ downlineSize: u.downlineSize });
        updatedDownlineSizes++;
      }
    });

    if (updatedDownlineLevels > 0) {
      log.info(`updated downlineLevel ${updatedDownlineLevels} times`);
    }
    if (updatedDownlineUsers > 0) {
      log.info(`updated downlineUsers ${updatedDownlineUsers} times`);
    }
    if (updatedDownlineSizes > 0) {
      log.info(`updated downlineSize ${updatedDownlineSizes} times`);
    }
  }

  unblockUsers() {
    let self = this;
    let reassigned = 0;

    let blockedUsers: any[] = _.filter(this.users, { state: 'blocked-by-upline' });
    _.each(blockedUsers, (u, uid) => {
      let sponsorState: string = this.sponsorState(u);
      let sponsor = this.getSponsor(u);
      if (sponsorState === 'missing-password-and-wallet' || (sponsorState == 'disabled' && this.getSponsor(u).fraudSuspected)) {
        log.info(`blockedUser userId=${u.userId}, state=${u.state}, sponsorState=${sponsorState}`);
        let newSponsor = _.find(self.uplineUsers(self.getSponsor(u)), (uplineUser, index) => {
          return index > 0 && !_.includes(blockingStates, uplineUser);
        });
        if (newSponsor) {
          // log.info(`${sponsor.email}\t${sponsor.state}\thttps://web.ur.technology/?admin-redirect=true&redirect=user&id=${sponsor.userId}`);
          u.oldSponsor = _.merge(_.clone(u.sponsor), { replacedAt: firebase.database.ServerValue.TIMESTAMP });
          u.newSponsor = _.pick(newSponsor, ['userId', 'name', 'profilePhotoUrl']);
          u.newSponsor.announcementTransactionConfirmed = !!newSponsor.wallet &&
            !!newSponsor.wallet.announcementTransaction &&
            !!newSponsor.wallet.announcementTransaction.blockNumber &&
            !!newSponsor.wallet.announcementTransaction.hash;
        }
      }
    });
    blockedUsers = _.filter(blockedUsers, 'newSponsor');
    _.each(blockedUsers, (u, uid) => {
      u.sponsor = u.newSponsor;
      self.userRef(u).update({ oldSponsor: u.oldSponsor, sponsor: u.sponsor }).then(() => {
        log.info(`reassigned u.userId: ${u.userId}, u.oldSponsor.userId: ${u.oldSponsor.userId}, u.newSponsor.userId: ${u.newSponsor.userId}`);
      });
      reassigned++;
    });

    self.fixDownlineInfo();
    log.info(`${reassigned} blocked users were reassigned`);
  }

  private referralUsers(u: any): any[] {
    return _.filter(this.users, (r: any) => {
      return r.sponsor && r.sponsor.userId === u.userId && r.userId !== u.userId
    });
  }

  private downlineUsers(u: any): any[] {
    if (!u.calculatedDownlineUsers) {
      let referrals: any[] = this.referralUsers(u);
      u.calculatedDownlineUsers = referrals.concat(_.flatten(_.map(referrals, (r) => { return this.downlineUsers(r); })));
    }
    return u.calculatedDownlineUsers;
  }

  private convertPushIdToTimestamp(id: string): number {
    let PUSH_CHARS = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    id = id.substring(0, 8);
    let timestamp = 0;
    for (let i = 0; i < id.length; i++) {
      var c = id.charAt(i);
      timestamp = timestamp * 64 + PUSH_CHARS.indexOf(c);
    }
    return timestamp;
  }

  private clearOldPhoneAuthTasks(): Promise<any[]> {
    let self = this;
    return new Promise((resolve, reject) => {
      let tasksRef = self.ref(`/phoneAuthQueue/tasks`);
      tasksRef.once('value', (snapshot: firebase.database.DataSnapshot) => {
        let phoneAuthTasks: any = snapshot.val() || {}
        _.each(phoneAuthTasks, (t, tid) => { t.taskId = tid; });
        log.info('done loading phoneAuthTasks');
        log.info('grouped by state', _.groupBy(phoneAuthTasks, '_state'));
        log.info('grouped by error', _.groupBy(phoneAuthTasks, '_error_details.error'));

        let count = 0;
        let now = new Date().valueOf();
        _.each(phoneAuthTasks, (t) => {
          let createdAt = this.convertPushIdToTimestamp(t.taskId);
          if ((now - createdAt) / 1000 / 60 / 60 > 0.5) {
            tasksRef.child(t.taskId).remove();
            count++;
          }
        });
        log.info(`removed ${count} phoneAuthTasks`);
        resolve(phoneAuthTasks);
      });
    });
  }

  private createdWithinWeeks(u: any, w1: number, w2: number): boolean {
    let now = new Date().valueOf();
    let weeksAgoCreated = (now - u.createdAt) / 1000 / 60 / 60 / 24 / 7;
    return w1 <= weeksAgoCreated && weeksAgoCreated < w2 && u.wallet && u.wallet.address;
  }

  private growth(weeksBack: number) {
    let sample = _.filter(this.users, (u: any) => { return this.createdWithinWeeks(u, weeksBack - 1, weeksBack); });
    let sampleUserIds = _.map(sample, 'userId');
    sample = _.reject(sample, (u: any) => { return _.includes(sampleUserIds, u.sponsor && u.sponsor.userId); })
    log.info(`sample=${_.size(sample)}`);

    let nextGen = _.flatten(_.map(sample, this.downlineUsers));
    nextGen = _.filter(nextGen, (u: any) => { return this.createdWithinWeeks(u, weeksBack - 2, weeksBack); });
    log.info(`nextGen=${_.size(nextGen)}`);
  }

  announceUsers() {
    let announceableUsers: any[] = _.filter(this.users, { state: 'waiting-for-announcement-to-be-queued' } );
    _.each(announceableUsers, (u: any, index: number) => {
      this.ref('/walletCreatedQueue/tasks').push({ userId: u.userId }).then(() => {
        log.info(`announced user ${this.userRef(u).toString()}`);
      })
    });
    log.info(`number of users announced: ${_.size(announceableUsers)}`);
  }

  disableFraudSuspectedUsers() {
    let fraudSuspectedUsers: any[] = _.filter(this.users, { state: 'fraud-suspected' });
    let numLeft = _.size(fraudSuspectedUsers);
    _.each(fraudSuspectedUsers, (u: any) => {
      log.info(`url=${this.userRef(u).toString()}`);
      this.userRef(u).update({ disabled: true }).then(() => {
        numLeft--;
        if (numLeft === 0) {
          log.info(`number of users disabled: ${_.size(fraudSuspectedUsers)}`);
        }
      });
    });
  }

  lookupCarrier(phone: string): Promise<any> {
    let self = this;
    return new Promise((resolve, reject) => {
      if (!self.twilioLookupsClient) {
        let twilio = require('twilio');
        self.twilioLookupsClient = new twilio.LookupsClient(self.env.TWILIO_ACCOUNT_SID, self.env.TWILIO_AUTH_TOKEN);
      }
      self.twilioLookupsClient.phoneNumbers(phone).get({
        type: 'carrier'
      }, function(error: any, number: any) {
        if (error) {
          reject(`error looking up carrier: ${error.message}`);
          return;
        }
        resolve({
          name: (number && number.carrier && number.carrier.name) || "Unknown",
          type: (number && number.carrier && number.carrier.type) || "Unknown"
        });
      });
    });
  }

  disableAndSignOutUser(u: any) {
    this.userRef(u).update({ disabled: true });
    this.signOutUser(u);
  }

  signOutUser(u: any) {
    this.auth.deleteUser(u.userId)
  }

  uplineUsers(u: any) {
    let upline: any[] = [];
    let currentUser: any = u;
    while (currentUser.sponsor) {
      currentUser = this.getSponsor(currentUser);
      upline.push(currentUser);
    }
    return upline;
  }

  checkNotifiedUsers() {
    let targetUsers: any[] = _.filter(this.users, 'marketingTests.1');
    log.info(`targetUsers count=${_.size(targetUsers)}`);
    let sent: any[] = _.filter(targetUsers, 'marketingTests.1.initiated');
    let sentWithBonus: any[] = _.filter(sent, { state: 'announcement-confirmed'});
    let notSent: any[] = _.reject(targetUsers, 'marketingTests.1.initiated');
    let notSentWithBonus: any[] = _.filter(notSent, { state: 'announcement-confirmed'});
    log.info(`sent total=${_.size(sent)}`);
    log.info(`sent with bonus=${_.size(sentWithBonus)}`);
    log.info(`notSent count=${_.size(notSent)}`);
    log.info(`notSent with bonus=${_.size(notSentWithBonus)}`);
  }

  notifyUsers() {
    let targetUsers: any[] = _.filter(this.users, { state: 'missing-docs' });
    log.info(`targetUsers count=${_.size(targetUsers)}`);

    let startTime: number = moment("2017-04-03 00:00:00-05:00").valueOf();
    let endTime: number = moment("2017-04-23 00:00:00-05:00").valueOf();
    targetUsers = _.filter(targetUsers, (u) => {
      return u.createdAt && u.createdAt >= startTime && u.createdAt <= endTime;
    });
    log.info(`targetUsers past apr 3 time count=${_.size(targetUsers)}`);

    targetUsers = _.filter(targetUsers, 'phone');
    log.info(`targetUsers with phone=${_.size(targetUsers)}`);

    let countries: any[] = require('country-data').countries.all;
    _.each(targetUsers, (u: any) => {
      if (!u.countryCode && u.phone) {
        let phoneNumberUtil: any = require('google-libphonenumber').PhoneNumberUtil.getInstance();
        try {
            // phone must begin with '+'
            let numberProto: any = phoneNumberUtil.parse(u.phone, "");
            let callingCode: any = numberProto.getCountryCode();
            if (callingCode) {
              let country = _.find(countries, (c) => {
                return _.includes(c.countryCallingCodes, `+${callingCode}`);
              });
              if (country) {
                u.countryCode = country.alpha2;
              }
            }
        } catch(e) {
          // log.info(`got exception ${e} for phone ${u.phone}`);
        }
      }
    });


    // targetUsers = _.filter(targetUsers, 'email');
    // log.info(`targetUsers with email=${_.size(targetUsers)}`);
    //
    // targetUsers = _.filter(targetUsers, 'name');
    // log.info(`targetUsers with name=${_.size(targetUsers)}`);

    this.notifyNextUser(targetUsers, 0);
  }

  notifyNextUser(targetUsers: any[], index: number) {
    if (index > targetUsers.length - 1) {
      return;
    }

    let u: any = targetUsers[index];

    if (u.marketingTests && ( u.marketingTests[1] || u.marketingTests["1"] )) {
      this.notifyNextUser(targetUsers, index + 1);
      return;
    }

    if (index % 2 === 0) {
      this.userRef(u).child('marketingTests/1').update({initiated: true});
      let message = "Your are one step away from receiving your free UR cryptocurrency. Visit web.ur.technology to complete signup and claim your UR.";
      this.sendSms(u.phone, message).then(() => {
        this.userRef(u).child('marketingTests/1').update({completed: true});
        this.notifyNextUser(targetUsers, index + 1);
      })
    } else {
      this.userRef(u).child('marketingTests/1').update({initiated: false});
      this.notifyNextUser(targetUsers, index + 1);
    }
  }

  notifyUsers2() {
    let oecdCountries: any = {
      'Australia': 'AU',
      'Austria': 'AT',
      'Belgium': 'BE',
      'Canada': 'CA',
      'Switzerland': 'CH',
      'Germany': 'DE',
      'Denmark': 'DK',
      'Spain': 'ES',
      'Finland': 'FI',
      'France': 'FR',
      'United Kingdom': 'GB',
      'Greece': 'GR',
      'Ireland': 'IE',
      'Iceland': 'IS',
      'Italy': 'IT',
      'Japan': 'JP',
      'Korea': 'KR',
      'Luxembourg': 'LU',
      'Mexico': 'MX',
      'Liechtenstein': 'FL',
      'Norway': 'NO',
      'New Zealand': 'NW',
      'Portugal': 'PT',
      'Sweden': 'SE',
      'United States': 'US',
      'Puerto Rico': 'US',
      'Netherlands': 'NL',
      'Republic of Korea': 'KR'
    };
    let oecdCountryCodes = _.values(oecdCountries);
    let targetUsers: any[] = _.filter(this.users, { state: 'missing-wallet' });
    log.info(`missing-wallet count=${_.size(targetUsers)}`);

    let countries: any[] = require('country-data').countries.all;

    targetUsers = _.filter(targetUsers, (u: any) => {
      let countryCode: string = u.countryCode;
      if (!countryCode && u.phone) {
        let phoneNumberUtil: any = require('google-libphonenumber').PhoneNumberUtil.getInstance();
        try {
            // phone must begin with '+'
            let numberProto: any = phoneNumberUtil.parse(u.phone, "");
            let callingCode: any = numberProto.getCountryCode();
            if (callingCode) {
              let country = _.find(countries, (c) => {
                return _.includes(c.countryCallingCodes, `+${callingCode}`);
              });
              if (country) {
                countryCode = country.alpha2;
              }
            }
        } catch(e) {
          // log.info(`got exception ${e} for phone ${u.phone}`);
        }
      }
      if (!countryCode && u.prefineryUser && u.prefineryUser.country) {
        let prefineryCountry: string = <string> u.prefineryUser.country;
        countryCode = oecdCountries[prefineryCountry];
      }
      return _.includes(oecdCountryCodes, countryCode);
    });

    log.info(`targetUsers=${_.size(targetUsers)}`);
    targetUsers = _.filter(targetUsers, 'email');
    log.info(`targetUsers with email=${_.size(targetUsers)}`);
    targetUsers = _.filter(targetUsers, 'phone');
    log.info(`targetUsers with phone=${_.size(targetUsers)}`);
    targetUsers = _.filter(targetUsers, 'name');
    log.info(`targetUsers with name=${_.size(targetUsers)}`);
    log.info(`targetUsers=${_.size(targetUsers)}`);

    // _.each(targetUsers, (u: any) => {
    //   this.sendSms(u.phone, "Your 2,000 UR Bonus is waiting for you. Please sign in and verify your account. https://web.ur.technology").then(() => {
    //     this.userRef(u).update({unblockEmailSent: true})
    //   })
    // });


    // var fs = require('fs');
    // var stream = fs.createWriteStream("my_file.csv");
    // stream.once('open', function(fd: any) {
    //   _.each(targetUsers, (u: any) => {
    //     stream.write(`=\"${u.userId}\",\"${_.trim(u.name)}\",\"${u.email}\",=\"${u.phone}\",\"${u.countryCode}\"\n`);
    //   });
    //   stream.end();
    // });

    // let count = 0;
    // countryCodes = _.sortBy(countryCodes, (countryCode) => {
    //   if (_.includes(oecdCountryCodes, countryCode)) {
    //     count = count + _.size(countryCodeGroups[countryCode]);
    //   }
    //   return -((_.includes(oecdCountryCodes, countryCode) ? 10000 : 0 ) + _.size(countryCodeGroups[countryCode]));
    // });
    // log.info(`count=${count}`);
    // _.each(countryCodes, (countryCode) => {
    //   log.info(`oecd=${_.includes(oecdCountryCodes, countryCode) ? 'Y' : ' '} / countryCode=${countryCode} / size=${_.size(countryCodeGroups[countryCode])}`)
    // });

  }

  showUserTransactions() {
    let u: any = this.users["-KXDMY4SKI5SbEEGXT3t"];
    this.userRef(u).child(`transactions`).once('value', (snapshot: firebase.database.DataSnapshot) => {
      let transactions: any[] = _.sortBy(_.filter(snapshot.val(), {type: 'sent'}), 'createdAt');
      log.info(`User ${u.name} - email: ${u.email}, id: ${u.userId}\n  number of send transactions: ${_.size(transactions)}`);
      _.each(transactions, (t) => {
        let amount: string = new BigNumber(t.amount).dividedBy(1000000000000000000).toFormat(2);
        let time: string = moment(t.createdAt).utcOffset(-300).format();
        log.info(`\n  txid: ${t.urTransaction.hash}\n  to: ${t.urTransaction.to}\n  amount: ${amount}\n  time: ${time}\n  blockNumber: ${t.urTransaction.blockNumber}\n  receiver: ${t.receiver.name}`);
      });
    });
  }

  displayStats() {
    let groups: any;

    log.info(`\ntotal users: ${_.size(this.users)}\n`);

    groups = _.groupBy(this.users, 'state');
    _.each(groups, (group, state) => {
      log.info(`  state: ${state}, count: ${_.size(group)}`);
      if (state === 'blocked-by-upline') {
        let subGroups: any = _.groupBy(group, (u) => {
          return this.rootUser(u).state;
        });
        _.each(subGroups, (subGroup: any[], subGroupState: string) => {
          log.info(`         blocking user state: ${subGroupState}, count: ${_.size(subGroup)}`);
          // _.each(subGroup, (u) => {
          //   log.info(`${u.email}\t${u.state}\thttps://web.ur.technology/?admin-redirect=true&redirect=user&id=${u.userId}`);
          // });
        });
      }
    });

    let confirmedUsers: any[] = _.filter(this.users, {state: 'bonus-approved'});
    log.info(`\nconfirmed user count by day (Central Time):`);
    let usersLeft = _.size(confirmedUsers)
    _.each(confirmedUsers, (u) => {
      let hash: number = u.wallet.announcementTransaction.hash;
      this.userRef(u).child(`transactions/${hash}/createdAt`).once('value', (snapshot: firebase.database.DataSnapshot) => {
        let createdAt: number = parseInt(snapshot.val());
        u.dayCreated = moment(createdAt).utcOffset(-300).startOf('day').format('YYYY-MM-DD');
        usersLeft--;
        if (usersLeft === 0) {
          let groups = _.groupBy(confirmedUsers, 'dayCreated');
          let days: string[] = _.keys(groups).sort();
          let total: number = 0;
          _.each(days, (dayCreated: string) => {
            let count: number = _.size(groups[dayCreated]);
            total = total + count;
            log.info(`${dayCreated}\t${count}\t${total}`);
          });
        }
      });
    });

  }

  signOutDisabledUsers() {
    let disabledUsers: any[] = _.filter(this.users, 'disabled');
    log.info(`count of disabledUsers=${_.size(disabledUsers)}`);
    this.disableNextBatchOfUsers(disabledUsers, 0);
  }

  disableNextBatchOfUsers(disabledUsers: any[], index: number) {
    let batchSize: number = 200;
    log.info(`batch index=${index}`);
    let lastIndex: number = _.min([index + batchSize - 1, disabledUsers.length - 1]);
    for (let i = index; i <= lastIndex; i++) {
      let finalizeBatch = () => {
        if (i === disabledUsers.length - 1) {
          let signedInUsers: any[] = _.filter(disabledUsers, 'signedIn');
          log.info(`${_.size(signedInUsers)} need to be signed out`);
          _.each(signedInUsers, (u: any) => {
            this.signOutUser(u);
            log.info(`signed out user ${u.userId}`);
          });
        } else if (i === lastIndex) {
          this.disableNextBatchOfUsers(disabledUsers, index+batchSize);
        }
      }

      let u: any = disabledUsers[i];
      this.auth.getUser(u.userId).then(() => {
        u.completed = true;
        u.signedIn = true;
        finalizeBatch();
      }, (error: any) => {
        u.completed = true;
        if (error && error.code === 'auth/user-not-found') {
          u.signedIn = false;
        } else {
          log.info(`error=${JSON.stringify(error)}`);
        }
        finalizeBatch();
      });
    }
  }

  displayDuplicateSignups() {
    let confirmedUsers: any[] = _.filter(this.users, {state: 'announcement-confirmed'});
    _.each(confirmedUsers, (u) => {
      this.userRef(u).child(`transactions`).once('value', (snapshot: firebase.database.DataSnapshot) => {
        let transactions: any = snapshot.val() || {};
        let signUpTransactions: any[] = _.filter(transactions, (t: any) => { return t.level === 0; });
        let num: number = _.size(signUpTransactions);
        if (num > 1) {
          log.info(`${num} sign-up transactions for user ${u.userId}`);
        }
      });
    });
  }
  openMissingFreshdeskTickets() {
    let usersWithReviewPending: any[] = _.filter(this.users, (u) => { return this.userState(u) === 'waiting-for-review'; });
    usersWithReviewPending = _.reject(usersWithReviewPending, 'freshdeskUrl');

    log.info(`about to open ${_.size(usersWithReviewPending)} tickets`);
    // _.each(usersWithReviewPending, (u) => { log.info(`  ${u.userId} / ${u.name} / ${u.email} / ${u.phone} / ${u.state} / ${u.downlineSize} / ${u.freshdeskUrl}`); });

    // let manualIDVerifier: ManualIDVerifier = new ManualIDVerifier( this.env, this.db );
    // _.each(usersWithReviewPending, (u: any) => {
    //   manualIDVerifier.openFreshdeskTicketIfNecessary(u.userId);
    // });
  }

  fixSendUrTransactions() {
    let potentiallyInvalidTransactions: any[] = require('../data/potentiallyInvalidTransactions.json');

    let Web3 = require('web3');
    let web3 = new Web3(new Web3.providers.HttpProvider('http://127.0.0.1:9595'));

    log.info(`about to check ${_.size(potentiallyInvalidTransactions)} potentially invalid hashes`);

    let chunk = _.slice(potentiallyInvalidTransactions, 0, 5000);
    let numLeft = _.size(chunk);
    _.each(chunk, (p, index) => {
      let ref = this.userRef({ userId: p.userId }).child(`transactions/${p.hash}`);
      ref.once('value', (snapshot: firebase.database.DataSnapshot) => {
        numLeft--;
        if (snapshot.exists()) {
          let retrievedTransaction: any = web3.eth.getTransaction(p.hash);
          if (retrievedTransaction) {
          } else {
            // ref.remove();
            log.info(`removed fb transaction data at ${ref.toString()}`);

            ref = this.userRef({ userId: p.userId }).child(`events/${p.hash}`);
            // ref.remove();
            log.info(`removed fb event at ${ref.toString()}`);
          }
        }
        if (numLeft === 0) {
          log.info('did them all');
        }
      });
    });
  }

  disableFraudulentDownline(email: string) {
    let matchedUsers: any[] = _.filter(this.users, { email: email });

    let count = _.size(matchedUsers);
    if (count === 0) {
      log.info(`found ${count} users with email ${email}`);
      return;
    }
    let downlines: any[] = _.flatten(_.map(matchedUsers, (m) => { return this.downlineUsers(m); }));

    let usersToDisable: any[] = matchedUsers.concat(downlines);

    _.each(usersToDisable, (u: any) => {
      this.userRef(u).update({ disabled: true, fraudSuspected: true }).then(() => {
        log.info(`successfully disabled user with email ${u.email}`);
      });
    });
  }

  private sendSms(phone: string, messageText: string): Promise<string> {
    let self = this;
    return new Promise((resolve, reject) => {
      if (!self.twilioRestClient) {
        let twilio = require('twilio');
        self.twilioRestClient = new twilio.RestClient(self.env.TWILIO_ACCOUNT_SID, self.env.TWILIO_AUTH_TOKEN);
      }
      self.twilioRestClient.messages.create({
        to: phone,
        from: self.env.TWILIO_FROM_NUMBER,
        body: messageText
      }, (error: any) => {
        if (error) {
          log.debug(`  error sending message '${messageText.substring(0,20)}' to ${phone}: ${error.message}`);
          reject(error);
          return;
        }

        log.debug(`  sent message '${messageText.substring(0,20)}' to ${phone}`);
        setTimeout(resolve, 250);
      });
    });
  }


}
