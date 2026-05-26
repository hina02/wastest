import { A } from "@solidjs/router";

export default function Navbar() {
  return (
    <nav class="fixed top-0 left-0 w-full bg-gray-50 border-b border-gray-200 text-gray-800 py-2 space-x-16 flex justify-center items-center shadow-xs">
      <div class="text-lg font-semibold">
        <A href="/">Home</A>
      </div>
      <div>
        <A href="/diary" activeClass="underlined" inactiveClass="default">
          Diary
        </A>
      </div>
      <div>
        <A href="/todo" activeClass="underlined" inactiveClass="default">
          Todo
        </A>
      </div>
    </nav>
  );
}
