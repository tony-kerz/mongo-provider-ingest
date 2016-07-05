'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _classCallCheck2 = require('babel-runtime/helpers/classCallCheck');

var _classCallCheck3 = _interopRequireDefault(_classCallCheck2);

var _createClass2 = require('babel-runtime/helpers/createClass');

var _createClass3 = _interopRequireDefault(_createClass2);

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var _class = function () {
  function _class(name) {
    (0, _classCallCheck3.default)(this, _class);

    this._name = name;
    this._count = 0;
    this._total = 0;
    this.start();
  }

  (0, _createClass3.default)(_class, [{
    key: 'start',
    value: function start() {
      this._start = process.hrtime();
    }
  }, {
    key: 'stop',
    value: function stop() {
      this._count++;
      this._last = millis(process.hrtime(this._start));
      if (_lodash2.default.isUndefined(this._min) || this._last < this._min) {
        this._min = this._last;
      }
      if (_lodash2.default.isUndefined(this._max) || this._last > this._max) {
        this._max = this._last;
      }
      this._total += this._last;
    }
  }, {
    key: 'min',
    value: function min() {
      return this._min;
    }
  }, {
    key: 'max',
    value: function max() {
      return this._max;
    }
  }, {
    key: 'avg',
    value: function avg() {
      return this._total / this._count;
    }
  }, {
    key: 'count',
    value: function count() {
      return this._count;
    }
  }, {
    key: 'name',
    value: function name() {
      return this._name;
    }
  }, {
    key: 'total',
    value: function total() {
      return this._total;
    }
  }, {
    key: 'toString',
    value: function toString() {
      return this._name + ': count=' + this._count + ', min=' + format(this._min) + ', max=' + format(this._max) + ', last=' + format(this._last) + ', avg=' + format(this.avg()) + ', total=' + format(this._total / 1000) + 's';
    }
  }]);
  return _class;
}();

exports.default = _class;


function format(n) {
  return n.toFixed(3);
}

function millis(t) {
  return t[0] * 1000 + t[1] / 1e6;
}