/* @refresh reload */
import { HashRouter, Route, type RouteSectionProps } from "@solidjs/router";
import { render } from "solid-js/web";
import Navbar from "$components/Navbar.tsx";
import Home from "$pages/Home.tsx";
import { greet } from "./wasm.ts";

const Layout = (props: RouteSectionProps) => {
  return (
    <>
      <header>
        <Navbar />
      </header>
      <p>{greet("World")}</p>
      {props.children}
      <footer></footer>
    </>
  );
};

render(
  () => (
    <>
      <HashRouter>
        <Route path="/" component={Layout}>
          <Route path="/" component={Home} />
        </Route>
      </HashRouter>
    </>
  ),
  document.getElementById("root")!,
);
