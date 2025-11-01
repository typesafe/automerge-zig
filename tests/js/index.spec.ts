import * as Automerge from "@automerge/automerge";
import { describe, it } from "bun:test";
import { writeFileSync,readFileSync } from "fs";
describe("AUtomerge", () => {
  it("AUtomerge", () => {
    // Set up the `cards` array in doc1
    // let doc1 = Automerge.change(Automerge.init(), (doc) => {
    //   doc.cards = [];
    // });
    // // In doc2, don't create `cards` again! Instead, merge
    // // the schema initialization from doc1
    // let doc2 = Automerge.merge(Automerge.init(), doc1);
    // // Now we can update both documents
    // doc1 = Automerge.change(doc1, (doc) => {
    //   doc.cards.push({ title: "card1" });
    // });
    // doc2 = Automerge.change(doc2, (doc) => {
    //   doc.cards.push({ title: "card2" });
    // });
    // // The merged document will contain both cards
    // doc1 = Automerge.merge(doc1, doc2);
    // doc2 = Automerge.merge(doc2, doc1);

    // const doc = Automerge.load(readFileSync("./tests/fixtures/exemplar"));
const doc = Automerge.init();

    console.log(doc);
    Automerge.change(doc, (d) => {
      d.prop = "val";
      // d.test = [];
      // d.test.push({ foo: "bar" });
    });

    const data = Automerge.save(doc);
    writeFileSync("test.am", data);
  });
});
