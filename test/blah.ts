import {expect} from 'chai';





describe('Blah', () => {

    let tests = [
        'some test'
    ];

    tests.forEach(test => {
        it(test, () => {

            let expected = 1;
            let actual = 2;
            expect(actual).equals(expected);
        });
    });
});
