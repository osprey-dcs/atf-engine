import asyncio
import logging
from . import main, getargs

if __name__=='__main__':
    args = getargs().parse_args()
    logging.basicConfig(level=args.level)
    asyncio.run(main(args), debug=args.debug)
